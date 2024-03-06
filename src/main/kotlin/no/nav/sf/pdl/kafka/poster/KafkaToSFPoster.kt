package no.nav.sf.pdl.kafka.poster

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.config_KAFKA_POLL_DURATION
import no.nav.sf.pdl.kafka.config_KAFKA_TOPIC
import no.nav.sf.pdl.kafka.config_POSTER_FLAGS
import no.nav.sf.pdl.kafka.env
import no.nav.sf.pdl.kafka.envAsFlags
import no.nav.sf.pdl.kafka.envAsLong
import no.nav.sf.pdl.kafka.kafka.KafkaConsumerFactory
import no.nav.sf.pdl.kafka.metrics.WorkSessionStatistics
import no.nav.sf.pdl.kafka.salesforce.KafkaMessage
import no.nav.sf.pdl.kafka.salesforce.SalesforceClient
import no.nav.sf.pdl.kafka.salesforce.isSuccess
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.io.File
import java.time.Duration
import java.util.Base64

const val NUMBER_OF_SAMPLES_IN_SAMPLE_RUN = 3
/**
 * KafkaToSFPoster
 * This class is responsible for handling a work session, ie polling and posting to salesforce until we are up-to-date with topic
 * Makes use of SalesforceClient to set up connection to salesforce
 */
class KafkaToSFPoster(
    private val filter: ((ConsumerRecord<String, String?>) -> Boolean)? = null,
    private val modifier: ((ConsumerRecord<String, String?>) -> String?)? = null,
    private val sfClient: SalesforceClient = SalesforceClient(),
    private val kafkaTopic: String = env(config_KAFKA_TOPIC),
    private val kafkaConsumerFactory: KafkaConsumerFactory = KafkaConsumerFactory(),
    private val kafkaPollDuration: Long = envAsLong(config_KAFKA_POLL_DURATION),
    flags: List<Flag> = envAsFlags(config_POSTER_FLAGS),
) {
    enum class Flag { DEFAULT, FROM_BEGINNING, NO_POST, SAMPLE, RUN_ONCE }

    private enum class ConsumeStatus { SUCCESSFULLY_CONSUMED_BATCH, NO_MORE_RECORDS, FAIL }

    private val log = KotlinLogging.logger { }

    // Flags set from settings
    private val hasFlagFromBeginning = flags.contains(Flag.FROM_BEGINNING)
    private val hasFlagNoPost = flags.contains(Flag.NO_POST)
    private val hasFlagSample = flags.contains(Flag.SAMPLE)
    private val hasFlagRunOnce = flags.contains(Flag.RUN_ONCE)

    private var samplesLeft = NUMBER_OF_SAMPLES_IN_SAMPLE_RUN
    private var hasRunOnce = false

    private lateinit var stats: WorkSessionStatistics

    fun runWorkSession() {
        if (hasFlagRunOnce && hasRunOnce) {
            log.info { "Work session skipped due to setting Only Run Once, and has consumed once" }
            return
        }

        stats = WorkSessionStatistics()

        // Instantiate each work session to fetch config from current state of environment (fetch injected updates of credentials)
        val kafkaConsumer = setupKafkaConsumer(kafkaTopic)

        hasRunOnce = true

        pollAndConsume(kafkaConsumer)
    }

    private fun setupKafkaConsumer(kafkaTopic: String): KafkaConsumer<String, String?> {
        return kafkaConsumerFactory.createConsumer().apply {
            // Using assign rather than subscribe since we need the ability to seek to a particular offset
            val topicPartitions = partitionsFor(kafkaTopic).map { TopicPartition(it.topic(), it.partition()) }
            assign(topicPartitions)
            log.info { "Starting work session on topic $kafkaTopic with ${topicPartitions.size} partitions" }
            if (hasFlagFromBeginning && !hasRunOnce) {
                seekToBeginning(emptyList()) // emptyList(): Seeks to the first offset for all provided partitions
            }
        }
    }

    private tailrec fun pollAndConsume(kafkaConsumer: KafkaConsumer<String, String?>) {
        val records = kafkaConsumer.poll(Duration.ofMillis(kafkaPollDuration))
            as ConsumerRecords<String, String?>

        if (consumeRecords(records) == ConsumeStatus.SUCCESSFULLY_CONSUMED_BATCH) {
            kafkaConsumer.commitSync() // Will update position of kafka consumer in kafka cluster
            pollAndConsume(kafkaConsumer)
        }
    }

    private fun consumeRecords(recordsFromTopic: ConsumerRecords<String, String?>): ConsumeStatus =
        if (recordsFromTopic.isEmpty) {
            if (!stats.hasConsumed()) {
                WorkSessionStatistics.subsequentWorkSessionsWithEventsCounter.clear()
                WorkSessionStatistics.workSessionsWithoutEventsCounter.inc()
                log.info { "Finished work session without consuming. Number of work sessions without events during lifetime of app: ${WorkSessionStatistics.workSessionsWithoutEventsCounter.get().toInt()}" }
            } else {
                WorkSessionStatistics.subsequentWorkSessionsWithEventsCounter.inc()
                log.info { "Finished work session with activity (subsequent ${WorkSessionStatistics.subsequentWorkSessionsWithEventsCounter.get().toInt()}). $stats" }
            }
            ConsumeStatus.NO_MORE_RECORDS
        } else {
            WorkSessionStatistics.workSessionsWithoutEventsCounter.clear()
            stats.incConsumed(recordsFromTopic.count())

            val recordsFiltered = filterRecords(recordsFromTopic)

            if (hasFlagSample) sampleRecords(recordsFiltered)

            if (recordsFiltered.count() == 0 || hasFlagNoPost) {
                // Either we have set a flag to not post to salesforce, or the filter ate all candidates -
                // consider it a successfully consumed batch without further action
                ConsumeStatus.SUCCESSFULLY_CONSUMED_BATCH
            } else {
                when (sfClient.postRecords(recordsFiltered.toKafkaMessagesSet()).isSuccess()) {
                    true -> {
                        stats.updatePostedStatistics(recordsFiltered)
                        ConsumeStatus.SUCCESSFULLY_CONSUMED_BATCH
                    }
                    false -> {
                        log.warn { "Failed when posting to SF - $stats" }
                        WorkSessionStatistics.failedSalesforceCallCounter.inc()
                        ConsumeStatus.FAIL
                    }
                }
            }
        }

    private fun filterRecords(records: ConsumerRecords<String, String?>): Iterable<ConsumerRecord<String, String?>> {
        val recordsPostFilter = filter?.run { records.filter { invoke(it) } } ?: records
        stats.incBlockedByFilter(records.count() - recordsPostFilter.count())
        return recordsPostFilter
    }

    private fun Iterable<ConsumerRecord<String, String?>>.toKafkaMessagesSet(): Set<KafkaMessage> {
        val kafkaMessages = this.map {
            KafkaMessage(
                CRM_Topic__c = it.topic(),
                CRM_Key__c = it.key(),
                CRM_Value__c = (modifier?.run { invoke(it) } ?: it.value())?.encodeB64()
            )
        }

        val uniqueKafkaMessages = kafkaMessages.toSet()
        val uniqueValueCount = uniqueKafkaMessages.count()
        if (kafkaMessages.size != uniqueValueCount) {
            log.warn { "Detected ${kafkaMessages.size - uniqueValueCount} duplicates in $kafkaTopic batch" }
        }
        return uniqueKafkaMessages
    }

    private fun sampleRecords(records: Iterable<ConsumerRecord<String, String?>>) {
        if (samplesLeft > 0) {
            records.forEach {
                if (samplesLeft-- > 0) {
                    File("/tmp/samplesFromTopic").appendText("KEY: ${it.key()}\nVALUE: ${it.value()}\n\n")
                    if (modifier != null) {
                        File("/tmp/samplesAfterModifier").appendText("KEY: ${it.key()}\nVALUE: ${modifier.invoke(it)}\n\n")
                    }
                    log.info { "Saved sample. Samples left: $samplesLeft" }
                }
            }
        }
    }

    private fun String.encodeB64(): String = Base64.getEncoder().encodeToString(this.toByteArray())
}
