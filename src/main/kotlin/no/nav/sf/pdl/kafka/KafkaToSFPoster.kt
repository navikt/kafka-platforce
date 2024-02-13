package no.nav.sf.pdl.kafka

import mu.KotlinLogging
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

const val CONTINUE: Boolean = true
const val STOP: Boolean = false
/**
 * KafkaToSFPoster
 * This class is responsible for handling a work session, ie polling and posting to salesforce until we are up-to-date with topic
 * Makes use of SalesforceClient to setup connection to salesforce
 */
class KafkaToSFPoster<K, V>(
    val modifier: ((ConsumerRecord<String, String>) -> String)? = null,
    val filter: ((ConsumerRecord<String, String>) -> Boolean)? = null
) {
    private val log = KotlinLogging.logger { }

    val sfClient = SalesforceClient()
    val settings = envAsSettings(env_POSTER_SETTINGS)

    enum class Settings { DEFAULT, FROM_BEGINNING, NO_POST, SAMPLE, RUN_ONCE }

    val fromBeginning = settings.contains(Settings.FROM_BEGINNING)
    val noPost = settings.contains(Settings.NO_POST)
    val sample = settings.contains(Settings.SAMPLE)
    var runOnce = settings.contains(Settings.RUN_ONCE)

    var samples = numberOfSamplesInSampleRun
    var hasRunOnce = false

    fun runWorkSession(kafkaTopic: String) {
        val kafkaTopics: List<String> = listOf()

        if (runOnce && hasRunOnce) {
            log.info { "Work session skipped due to setting Only Run Once, and has consumed once" }
            return
        }
        val firstOffsetPosted: MutableMap<Int, Long> = mutableMapOf()

        /** First offset posted per kafka partition **/
        val lastOffsetPosted: MutableMap<Int, Long> = mutableMapOf()

        /** Last offset posted per kafka partition **/
        var consumedInCurrentRun = 0
        var pastFilterInCurrentRun = 0
        var uniqueToPost = 0

        // Instantiate each work session to fetch config from current state of environment (fetch injected updates of credentials)
        val kafkaConsumer = KafkaConsumer<String, String>(propertiesPlain).apply {
            // Using assign rather than subscribe since we need the ability to seek to a particular offset
            assign(kafkaTopics.flatMap { partitionsFor(it) }.map { TopicPartition(it.topic(), it.partition()) })
            if (fromBeginning) {
                seekToBeginning(emptyList()) // emptyList(): Seeks to the first offset for all provided partitions
            }
        }

        var pollStatus = CONTINUE

        while (pollStatus) {
            val records = kafkaConsumer.poll(Duration.ofMillis(envAsLong(env_KAFKA_POLL_DURATION)))
                as ConsumerRecords<String, String>
            hasRunOnce = true
            pollStatus = if (records.isEmpty) {
                if (consumedInCurrentRun == 0) {
                    log.info { "Work: Finished session without consuming. Number of work sessions without events during lifetime of app: $numberOfWorkSessionsWithoutEvents" }
                } else {
                    log.info {
                        "Work: Finished session with activity. $consumedInCurrentRun consumed $kafkaTopics records - past filter $pastFilterInCurrentRun - unique $uniqueToPost, posted offset range: ${
                        offsetMapsToText(firstOffsetPosted, lastOffsetPosted)
                        }"
                    }
                }
                STOP
            } else {
                numberOfWorkSessionsWithoutEvents = 0
                kCommonMetrics.noOfConsumedEvents.inc(records.count().toDouble())
                val recordsPostFilter = filter?.let { filter -> records.filter { filter(it) } } ?: records

                kCommonMetrics.noOfEventsBlockedByFilter.inc((records.count() - recordsPostFilter.count()).toDouble())
                consumedInCurrentRun += records.count()
                pastFilterInCurrentRun += recordsPostFilter.count()
                if (sample && samples > 0) {
                    recordsPostFilter.forEach {
                        if (samples > 0) {
                            File("/tmp/samples-$kafkaTopic").appendText("KEY: ${it.key()}\nVALUE: ${it.value()}\n\n")
                            if (modifier != null) {
                                File("/tmp/samplesAfterModifier-$kafkaTopic").appendText(
                                    "KEY: ${it.key()}\nVALUE: ${
                                    modifier.invoke(
                                        it
                                    )
                                    }\n\n"
                                )
                            }
                            samples--
                            log.info { "Saved sample. Samples left: $samples" }
                        }
                    }
                }

                val kafkaMessages = recordsPostFilter.map {
                    KafkaMessage(
                        CRM_Topic__c = it.topic(),
                        CRM_Key__c = it.key().toString(),
                        CRM_Value__c = (modifier?.let { modifier -> modifier(it) } ?: it.value().toString()).encodeB64()
                    )
                }

                val uniqueValueCount = kafkaMessages.toSet().count()
                if (kafkaMessages.size != uniqueValueCount) {
                    log.info { "Detected ${kafkaMessages.size - uniqueValueCount} duplicates in $kafkaTopic batch" }
                }
                uniqueToPost += uniqueValueCount

                val body = SFsObjectRest(
                    records = kafkaMessages.toSet().toList()
                ).toJson()
                if (recordsPostFilter.count() == 0 || noPost) {
                    CONTINUE
                } else {
                    when (sfClient(body).isSuccess()) {
                        true -> {
                            kCommonMetrics.noOfPostedEvents.inc(recordsPostFilter.count().toDouble())
                            if (!firstOffsetPosted.containsKey(
                                    recordsPostFilter.first().partition()
                                )
                            ) firstOffsetPosted[recordsPostFilter.first().partition()] =
                                recordsPostFilter.first().offset()
                            lastOffsetPosted[recordsPostFilter.last().partition()] = recordsPostFilter.last().offset()
                            recordsPostFilter.forEach {
                                kCommonMetrics.latestPostedOffset.labels(
                                    it.partition().toString()
                                ).set(it.offset().toDouble())
                            }
                            CONTINUE
                        }
                        false -> {
                            log.warn { "Failed when posting to SF" }
                            kCommonMetrics.producerIssues.inc()
                            STOP
                        }
                    }
                }
            }
        }
    }
}
