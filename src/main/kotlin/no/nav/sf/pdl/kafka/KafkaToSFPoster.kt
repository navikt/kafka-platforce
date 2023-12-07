package no.nav.sf.pdl.kafka

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.kafka.AKafkaConsumer
import no.nav.sf.pdl.kafka.kafka.KafkaConsumerStates
import no.nav.sf.pdl.kafka.metrics.kCommonMetrics
import no.nav.sf.pdl.kafka.metrics.numberOfWorkSessionsWithoutEvents
import no.nav.sf.pdl.kafka.salesforce.KafkaMessage
import no.nav.sf.pdl.kafka.salesforce.SFsObjectRest
import no.nav.sf.pdl.kafka.salesforce.SalesforceClient
import no.nav.sf.pdl.kafka.salesforce.isSuccess
import org.apache.avro.generic.GenericRecord
import java.io.File

/**
 * KafkaToSFPoster
 * This class is responsible for handling a work session, ie polling and posting to salesforce until we are up-to-date with topic
 * Makes use of SalesforceClient to setup connection to salesforce
 * Makes use of AKafkaConsumer to perform polling. This class provides code for how to process each capture batch
 * (Returns KafkaConsumerStates.IsOk only when we are sure the data has been sent )
 */
class KafkaToSFPoster<K, V>(
    val settings: List<Settings> = listOf(),
    val modifier: ((String, Long) -> String)? = null,
    val filter: ((String, Long) -> Boolean)? = null
) {
    private val log = KotlinLogging.logger { }

    enum class Settings {
        DEFAULT, FROM_BEGINNING, NO_POST, SAMPLE, RUN_ONCE, ENCODE_KEY, AVRO_KEY_VALUE, AVRO_VALUE
    }
    val sfClient = SalesforceClient()

    val fromBeginning = settings.contains(Settings.FROM_BEGINNING)
    val noPost = settings.contains(Settings.NO_POST)
    val sample = settings.contains(Settings.SAMPLE)
    var runOnce = settings.contains(Settings.RUN_ONCE)
    val encodeKey = settings.contains(Settings.ENCODE_KEY)
    val avroKeyValue = settings.contains(Settings.AVRO_KEY_VALUE)
    val avroValue = settings.contains(Settings.AVRO_VALUE)

    var samples = numberOfSamplesInSampleRun
    var hasRunOnce = false
    fun runWorkSession(kafkaTopic: String) {
        if (runOnce && hasRunOnce) {
            log.info { "Work session skipped due to setting Only Run Once, and has consumed once" }
            return
        }
        // kCommonMetrics.clearWorkSessionMetrics()
        var firstOffsetPosted: MutableMap<Int, Long> = mutableMapOf() /** First offset posted per kafka partition **/
        var lastOffsetPosted: MutableMap<Int, Long> = mutableMapOf() /** Last offset posted per kafka partition **/
        var consumedInCurrentRun = 0
        var pastFilterInCurrentRun = 0
        var uniqueToPost = 0

        val kafkaConsumerConfig = if (avroKeyValue) AKafkaConsumer.configAvro else if (avroValue) AKafkaConsumer.configAvroValueOnly else AKafkaConsumer.configPlain
        // Instansiate each time to fetch config from current state of environment (fetch injected updates of credentials etc):

        val consumer = if (avroKeyValue) {
            AKafkaConsumer<GenericRecord, GenericRecord>(kafkaConsumerConfig, kafkaTopic, envAsLong(env_KAFKA_POLL_DURATION), fromBeginning, hasRunOnce)
        } else if (avroValue) {
            AKafkaConsumer<K, GenericRecord>(kafkaConsumerConfig, kafkaTopic, envAsLong(env_KAFKA_POLL_DURATION), fromBeginning, hasRunOnce)
        } else {
            AKafkaConsumer<K, V>(kafkaConsumerConfig, kafkaTopic, envAsLong(env_KAFKA_POLL_DURATION), fromBeginning, hasRunOnce)
        }

        sfClient.enablesObjectPost { postActivities ->
            val isOk = consumer.consume { cRecordsPreFilter ->
                hasRunOnce = true
                if (cRecordsPreFilter.isEmpty) {
                    if (consumedInCurrentRun == 0) {
                        log.info { "Work: Finished session without consuming. Number if work sessions without event during lifetime of app: $numberOfWorkSessionsWithoutEvents" }
                    } else {
                        log.info { "Work: Finished session with activity. $consumedInCurrentRun consumed $kafkaTopic records - past filter $pastFilterInCurrentRun - unique $uniqueToPost, posted offset range: ${offsetMapsToText(firstOffsetPosted, lastOffsetPosted)}" }
                    }
                    KafkaConsumerStates.IsFinished
                } else {
                    numberOfWorkSessionsWithoutEvents = 0
                    kCommonMetrics.noOfConsumedEvents.inc(cRecordsPreFilter.count().toDouble())
                    val cRecords = if (filter == null) cRecordsPreFilter else cRecordsPreFilter.filter { filter!!(it.value().toString(), it.offset()) }
                    kCommonMetrics.noOfEventsBlockedByFilter.inc((cRecordsPreFilter.count() - cRecords.count()).toDouble())
                    consumedInCurrentRun += cRecordsPreFilter.count()
                    pastFilterInCurrentRun += cRecords.count()
                    if (sample && samples > 0) {
                        cRecords.forEach {
                            if (samples > 0) {
                                File("/tmp/samples-$kafkaTopic").appendText("KEY: ${it.key()}\nVALUE: ${it.value()}\n\n")
                                if (modifier != null) {
                                    File("/tmp/samplesAfterModifier-$kafkaTopic").appendText("KEY: ${it.key()}\nVALUE: ${modifier.invoke(it.value().toString(), it.offset())}\n\n")
                                }
                                samples--
                                log.info { "Saved sample. Samples left: $samples" }
                            }
                        }
                    }

                    val kafkaMessages = cRecords.map {
                        KafkaMessage(
                            CRM_Topic__c = it.topic(),
                            CRM_Key__c = if (encodeKey) it.key().toString().encodeB64() else it.key().toString(),
                            CRM_Value__c = it.value().toString()
                                .let { value -> if (modifier == null) value.toString().encodeB64() else modifier.invoke(value.toString(), it.offset()).encodeB64() }
                        )
                    }

                    val uniqueValueCount = kafkaMessages.toSet().count()
                    if (kafkaMessages.size != uniqueValueCount) {
                        log.info { "Detected ${kafkaMessages.size - uniqueValueCount} duplicates in $kafkaTopic batch" }
                    }

                    uniqueToPost += uniqueValueCount

                    val body = SFsObjectRest(
                        records = kafkaMessages
                    ).toJson()
                    if (noPost) {
                        KafkaConsumerStates.IsOk
                    } else {
                        when (postActivities(body).isSuccess()) {
                            true -> {
                                kCommonMetrics.noOfPostedEvents.inc(cRecords.count().toDouble())
                                if (!firstOffsetPosted.containsKey(cRecords.first().partition())) firstOffsetPosted[cRecords.first().partition()] = cRecords.first().offset()
                                lastOffsetPosted[cRecords.last().partition()] = cRecords.last().offset()
                                cRecords.forEach { kCommonMetrics.latestPostedOffset.labels(it.partition().toString()).set(it.offset().toDouble()) }
                                KafkaConsumerStates.IsOk
                            }
                            false -> {
                                log.warn { "Failed when posting to SF" }
                                kCommonMetrics.producerIssues.inc()
                                KafkaConsumerStates.HasIssues
                            }
                        }
                    }
                }
            }
            if (!isOk) {
                // Consumer issues is expected due to rotation of credentials, relocating app by kubernetes etc and is not critical.
                // As long as we do not commit offset until we sent the data it will be sent at next attempt
                kCommonMetrics.consumerIssues.inc()
                log.warn { "Kafka consumer reports NOK" }
            }
        }
        if (consumedInCurrentRun == 0) numberOfWorkSessionsWithoutEvents++
    }
}
