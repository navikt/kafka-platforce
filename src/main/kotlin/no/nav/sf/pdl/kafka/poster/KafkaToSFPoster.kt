package no.nav.sf.pdl.kafka.poster

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.NUMBER_OF_SAMPLES_IN_SAMPLE_RUN
import no.nav.sf.pdl.kafka.encodeB64
import no.nav.sf.pdl.kafka.env
import no.nav.sf.pdl.kafka.envAsLong
import no.nav.sf.pdl.kafka.envAsSettings
import no.nav.sf.pdl.kafka.env_KAFKA_POLL_DURATION
import no.nav.sf.pdl.kafka.env_KAFKA_TOPIC_PERSONDOKUMENT
import no.nav.sf.pdl.kafka.env_POSTER_SETTINGS
import no.nav.sf.pdl.kafka.kafka.propertiesPlain
import no.nav.sf.pdl.kafka.metrics.kCommonMetrics
import no.nav.sf.pdl.kafka.metrics.numberOfWorkSessionsWithoutEvents
import no.nav.sf.pdl.kafka.salesforce.KafkaMessage
import no.nav.sf.pdl.kafka.salesforce.SFsObjectRest
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
 * Makes use of SalesforceClient to setup connection to salesforce
 */
class KafkaToSFPoster<K, V>(
    val filter: ((ConsumerRecord<String, String?>) -> Boolean)? = null,
    val modifier: ((ConsumerRecord<String, String?>) -> String?)? = null,
    kafkaTopic: String = env(env_KAFKA_TOPIC_PERSONDOKUMENT),
    settings: List<Settings> = envAsSettings(env_POSTER_SETTINGS)
) {
    enum class Settings { DEFAULT, FROM_BEGINNING, NO_POST, SAMPLE, RUN_ONCE }

    private enum class ConsumeStatus { START, SUCCESSFULLY_CONSUMED_BATCH, NO_MORE_RECORDS, FAIL }

    private val log = KotlinLogging.logger { }

    private val hasFlagFromBeginning = settings.contains(Settings.FROM_BEGINNING)
    private val hasFlagNoPost = settings.contains(Settings.NO_POST)
    private val hasFlagSample = settings.contains(Settings.SAMPLE)
    private var hasFlagRunOnce = settings.contains(Settings.RUN_ONCE)

    private val sfClient = SalesforceClient()

    private var samplesLeft = NUMBER_OF_SAMPLES_IN_SAMPLE_RUN
    private var hasRunOnce = false

    val kafkaConsumer = KafkaConsumer<String, String>(propertiesPlain).apply {
        // Using assign rather than subscribe since we need the ability to seek to a particular offset
        val topicPartitions = partitionsFor(kafkaTopic).map { TopicPartition(it.topic(), it.partition()) }
        assign(topicPartitions)
        log.info { "Starting work session on topic $kafkaTopic with ${topicPartitions.size} partitions" }
        if (hasFlagFromBeginning) {
            seekToBeginning(emptyList()) // emptyList(): Seeks to the first offset for all provided partitions
        }
    }

    fun runWorkSession(kafkaTopic: String) {
        if (hasFlagRunOnce && hasRunOnce) {
            log.info { "Work session skipped due to setting Only Run Once, and has consumed once" }
            return
        }

        val stats = WorkSessionStatistics()

        var status = ConsumeStatus.START

        while (status == ConsumeStatus.START || status == ConsumeStatus.SUCCESSFULLY_CONSUMED_BATCH) {
            val recordsFromTopic = kafkaConsumer.poll(Duration.ofMillis(envAsLong(env_KAFKA_POLL_DURATION)))
                as ConsumerRecords<String, String>
            hasRunOnce = true
            status = if (recordsFromTopic.isEmpty) {
                if (status == ConsumeStatus.START) {
                    log.info { "Work: Finished session without consuming. Number of work sessions without events during lifetime of app: $numberOfWorkSessionsWithoutEvents" }
                } else {
                    log.info { "Work: Finished session with activity. $stats" }
                }
                ConsumeStatus.NO_MORE_RECORDS
            } else {
                numberOfWorkSessionsWithoutEvents = 0
                kCommonMetrics.noOfConsumedEvents.inc(recordsFromTopic.count().toDouble())
                val recordsPostFilter = filter?.let { filter -> recordsFromTopic.filter { filter(it) } } ?: recordsFromTopic
                val blockedByFilterInBatch = recordsFromTopic.count() - recordsPostFilter.count()
                stats.blockedByFilter += blockedByFilterInBatch
                kCommonMetrics.noOfEventsBlockedByFilter.inc(blockedByFilterInBatch.toDouble())
                stats.consumed += recordsFromTopic.count()

                if (hasFlagSample) sample(recordsPostFilter)

                if (recordsPostFilter.count() == 0 || hasFlagNoPost) {
                    if (recordsFromTopic.count() > 0 && recordsFromTopic.any { it.value() == null }) {
                        val kafkaMessages = recordsPostFilter.map {
                            KafkaMessage(
                                CRM_Topic__c = it.topic(),
                                CRM_Key__c = it.key().toString(),
                                CRM_Value__c = (
                                    modifier?.let { modifier -> modifier(it) } ?: it.value()?.toString()
                                        ?.encodeB64()
                                    )
                            )
                        }

                        val requestBody = SFsObjectRest(
                            records = kafkaMessages.toSet().toList()
                        ).toJson()

                        File("/tmp/exampleBatchWithTombstone").writeText(requestBody)
                    }
                    ConsumeStatus.SUCCESSFULLY_CONSUMED_BATCH
                } else {
                    val kafkaMessages = recordsPostFilter.map {
                        KafkaMessage(
                            CRM_Topic__c = it.topic(),
                            CRM_Key__c = it.key().toString(),
                            CRM_Value__c = (modifier?.let { modifier -> modifier(it) } ?: it.value()?.toString()?.encodeB64())
                        )
                    }

                    val uniqueValueCount = kafkaMessages.toSet().count()
                    if (kafkaMessages.size != uniqueValueCount) {
                        log.warn { "Detected ${kafkaMessages.size - uniqueValueCount} duplicates in $kafkaTopic batch" }
                    }
                    stats.postedUniqueRecords += uniqueValueCount

                    val requestBody = SFsObjectRest(
                        records = kafkaMessages.toSet().toList()
                    ).toJson()

                    when (sfClient(requestBody).isSuccess()) {
                        true -> {
                            kCommonMetrics.noOfPostedEvents.inc(recordsPostFilter.count().toDouble())
                            stats.postedOffsets.update(recordsPostFilter.toList())
                            ConsumeStatus.NO_MORE_RECORDS
                        }

                        false -> {
                            log.warn { "Failed when posting to SF" }
                            kCommonMetrics.producerIssues.inc()
                            ConsumeStatus.FAIL
                        }
                    }
                }
            }
        }
    }

    fun sample(records: Iterable<ConsumerRecord<String, String?>>) {
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
