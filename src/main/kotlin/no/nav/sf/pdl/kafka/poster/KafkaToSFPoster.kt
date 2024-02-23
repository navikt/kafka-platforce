package no.nav.sf.pdl.kafka.poster

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.NUMBER_OF_SAMPLES_IN_SAMPLE_RUN
import no.nav.sf.pdl.kafka.encodeB64
import no.nav.sf.pdl.kafka.env
import no.nav.sf.pdl.kafka.envAsFlags
import no.nav.sf.pdl.kafka.envAsLong
import no.nav.sf.pdl.kafka.env_KAFKA_POLL_DURATION
import no.nav.sf.pdl.kafka.env_KAFKA_TOPIC_PERSONDOKUMENT
import no.nav.sf.pdl.kafka.env_POSTER_FLAGS
import no.nav.sf.pdl.kafka.kafka.propertiesPlain
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

/**
 * KafkaToSFPoster
 * This class is responsible for handling a work session, ie polling and posting to salesforce until we are up-to-date with topic
 * Makes use of SalesforceClient to set up connection to salesforce
 */
class KafkaToSFPoster(
    private val filter: ((ConsumerRecord<String, String?>) -> Boolean)? = null,
    private val modifier: ((ConsumerRecord<String, String?>) -> String?)? = null,
    private val sfClient: SalesforceClient = SalesforceClient(),
    private val kafkaTopic: String = env(env_KAFKA_TOPIC_PERSONDOKUMENT),
    flags: List<Flag> = envAsFlags(env_POSTER_FLAGS)
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
        hasRunOnce = true

        stats = WorkSessionStatistics()

        val kafkaConsumer = setupKafkaConsumer(kafkaTopic)

        pollAndConsume(kafkaConsumer)
    }

    private fun setupKafkaConsumer(kafkaTopic: String): KafkaConsumer<String, String?> {
        return KafkaConsumer<String, String?>(propertiesPlain).apply {
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
        val records = kafkaConsumer.poll(Duration.ofMillis(envAsLong(env_KAFKA_POLL_DURATION)))
            as ConsumerRecords<String, String?>

        if (consumeRecords(records) == ConsumeStatus.SUCCESSFULLY_CONSUMED_BATCH) {
            kafkaConsumer.commitSync() // Will update position of kafka consumer in kafka cluster
            pollAndConsume(kafkaConsumer)
        }
    }

    private fun consumeRecords(recordsFromTopic: ConsumerRecords<String, String?>): ConsumeStatus =
        if (recordsFromTopic.isEmpty) {
            if (!stats.hasConsumed()) {
                WorkSessionStatistics.workSessionsWithoutEventsCounter.inc()
                log.info { "Finished work session without consuming. Number of work sessions without events during lifetime of app: ${WorkSessionStatistics.workSessionsWithoutEventsCounter.get().toInt()}" }
            } else {
                log.info { "Finished work session with activity. $stats" }
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
        val recordsPostFilter = filter?.run { records.filter { this(it) } } ?: records
        stats.incBlockedByFilter(records.count() - recordsPostFilter.count())
        return recordsPostFilter
    }

    private fun Iterable<ConsumerRecord<String, String?>>.toKafkaMessagesSet(): Set<KafkaMessage> {
        val kafkaMessages = this.map {
            KafkaMessage(
                CRM_Topic__c = it.topic(),
                CRM_Key__c = it.key(),
                CRM_Value__c = (modifier?.run { this(it) } ?: it.value()?.encodeB64())
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
}
