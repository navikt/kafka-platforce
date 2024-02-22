package no.nav.sf.pdl.kafka.metrics

import no.nav.sf.pdl.kafka.metrics.Prometheus.registerCounter
import no.nav.sf.pdl.kafka.metrics.Prometheus.registerGauge
import no.nav.sf.pdl.kafka.metrics.Prometheus.registerLabelGauge
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * This class stores session statistics for logging and monitoring purposes.
 */
data class WorkSessionStatistics(
    /**
     * Statistics for a single work session
     */
    private var consumed: Int = 0,
    private var blockedByFilter: Int = 0,
    private var posted: Int = 0,
    private val postedOffsets: PostedOffsets = PostedOffsets() // To log what range of offsets per partition has been posted
) {
    /**
     * Statistics for the lifetime of the app
     */
    companion object Metrics {
        private val consumedCounter = registerCounter("consumed")
        private val postedCounter = registerCounter("posted")
        private val latestPostedOffsetGauge = registerLabelGauge("latest_posted_offset", "partition")
        private val blockedByFilterGauge = registerGauge("blocked_by_filter")
        val workSessionsWithoutEventsCounter = registerCounter("work_sessions_without_events")
        val failedSalesforceCallCounter = registerCounter("failed_salesforce_call")
        val workSessionExceptionCounter = registerCounter("work_session_exception")
    }

    fun hasConsumed(): Boolean {
        return consumed != 0
    }

    fun incConsumed(value: Int) {
        consumed += value
        consumedCounter.inc(value.toDouble())
    }

    fun incBlockedByFilter(value: Int) {
        blockedByFilter += value
        blockedByFilterGauge.inc(value.toDouble())
    }

    fun updatePostedStatistics(postedRecords: Iterable<ConsumerRecord<String, String?>>) {
        val count = postedRecords.count()
        posted += count
        postedCounter.inc(count.toDouble())
        postedRecords.forEach {
            if (!postedOffsets.firstOffsetPostedPerPartition.containsKey(it.partition())) {
                postedOffsets.firstOffsetPostedPerPartition[it.partition()] = it.offset()
            }
            postedOffsets.lastOffsetPostedPerPartition[it.partition()] = it.offset()
        }
        postedOffsets.lastOffsetPostedPerPartition.forEach { entry ->
            latestPostedOffsetGauge.labels(entry.key.toString()).set(entry.value.toDouble())
        }
    }

    data class PostedOffsets(
        val firstOffsetPostedPerPartition: MutableMap<Int, Long> = mutableMapOf(),
        val lastOffsetPostedPerPartition: MutableMap<Int, Long> = mutableMapOf()
    ) {
        override fun toString(): String {
            if (firstOffsetPostedPerPartition.isEmpty()) return "NONE"
            return firstOffsetPostedPerPartition.keys.sorted().joinToString(",") {
                "$it:[${firstOffsetPostedPerPartition[it]}-${lastOffsetPostedPerPartition[it]}]"
            }
        }
    }
}
