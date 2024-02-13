package no.nav.sf.pdl.kafka.metrics

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Gauge
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging

object Metrics {
    private val log = KotlinLogging.logger { }
    val cRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    fun registerGauge(name: String): Gauge {
        return Gauge.build().name(name).help(name).register()
    }
    fun registerLabelGauge(name: String, label: String): Gauge {
        return Gauge.build().name(name).help(name).labelNames(label).register()
    }
    init {
        DefaultExports.initialize()
        log.info { "Prometheus metrics are ready" }
    }
}

data class KCommonMetrics(
    val pollErrorAuthentication: Gauge = Metrics.registerGauge("pollErrorAuthentication"),
    val pollErrorAuthorization: Gauge = Metrics.registerGauge("pollErrorAuthorization"),
    val pollErrorDeserialization: Gauge = Metrics.registerGauge("pollErrorDeserialization"),
    val commitErrorTimeBetweenPolls: Gauge = Metrics.registerGauge("commitErrorTimeBetweenPolls"),
    val consumeErrorServiceUnavailable: Gauge = Metrics.registerGauge("consumeErrorServiceUnavailable"),
    val unknownErrorConsume: Gauge = Metrics.registerGauge("unknownErrorConsume"),
    val unknownErrorPoll: Gauge = Metrics.registerGauge("unknownErrorPoll"),
    val unknownErrorCommit: Gauge = Metrics.registerGauge("unknownErrorCommit"),
    val noOfConsumedEvents: Gauge = Metrics.registerGauge("kafka_consumed_event_gauge"),
    val noOfEventsBlockedByFilter: Gauge = Metrics.registerGauge("kafka_blocked_by_filter_gauge"),
    val noOfPostedEvents: Gauge = Metrics.registerGauge("sf_posted_event_gauge"),
    val producerIssues: Gauge = Metrics.registerGauge("producer_issues"),
    val consumerIssues: Gauge = Metrics.registerGauge("consumer_issues"),
    val latestPostedOffset: Gauge = Metrics.registerLabelGauge("latest_posted_offset", "partition")
)

fun KCommonMetrics.clearWorkSessionMetrics() {
    noOfConsumedEvents.clear()
    noOfPostedEvents.clear()
}

val kCommonMetrics = KCommonMetrics()
var kErrorState = ErrorState.NONE
var currentConsumerMessageHost = "DEFAULT"
var kafkaConsumerOffsetRangeBoard: MutableMap<String, Pair<Long, Long>> = mutableMapOf()
var numberOfWorkSessionsWithoutEvents = 0

const val POSTFIX_FAIL = "-FAIL"
const val POSTFIX_FIRST = "-FIRST"
const val POSTFIX_LATEST = "-LATEST"

enum class ErrorState() {
    NONE, UNKNOWN_ERROR, AUTHORIZATION, AUTHENTICATION, DESERIALIZATION, TIME_BETWEEN_POLLS,
    SERVICE_UNAVAILABLE, TOPIC_ASSIGNMENT
}
