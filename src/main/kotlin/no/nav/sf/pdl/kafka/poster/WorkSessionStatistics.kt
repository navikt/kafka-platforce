package no.nav.sf.pdl.kafka.poster

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * A data classes for holding some session statistics for logging purposes
 */
data class WorkSessionStatistics(
    var consumedInCurrentRun: Int = 0,
    var pastFilterInCurrentRun: Int = 0,
    var uniqueRecords: Int = 0,
    val postedOffsets: PostedOffsets = PostedOffsets() // To log what range of offsets per partition has been posted
)

data class PostedOffsets(
    val firstOffsetPostedPerPartition: MutableMap<Int, Long> = mutableMapOf(),
    val lastOffsetPostedPerPartition: MutableMap<Int, Long> = mutableMapOf()
)

fun PostedOffsets.toString(): String {
    if (firstOffsetPostedPerPartition.isEmpty()) return "NONE"
    return firstOffsetPostedPerPartition.keys.sorted().map {
        "$it:[${firstOffsetPostedPerPartition[it]}-${lastOffsetPostedPerPartition[it]}]"
    }.joinToString(",")
}

fun PostedOffsets.update(records: List<ConsumerRecord<String, String>>) {
    records.forEach {
        if (!firstOffsetPostedPerPartition.containsKey(it.partition())) {
            firstOffsetPostedPerPartition[it.partition()] = it.offset()
        }
        lastOffsetPostedPerPartition[it.partition()] = it.offset()
    }
    // kCommonMetrics.latestPostedOffset.labels(partition.toString()).set(offset.toDouble())
}
