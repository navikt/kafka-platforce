import no.nav.sf.pdl.kafka.metrics.Prometheus
import no.nav.sf.pdl.kafka.metrics.WorkSessionStatistics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class WorkSessionStatisticsTest {
    @Test
    fun `Test that consume work sessions statistics and lifetime metrics are updated as expected`() {
        var workSessionStatistics = WorkSessionStatistics()

        // Initial state is reflected in .toString() and in metrics
        assertEquals(
            "WorkSessionStatistics(consumed=0, blockedByFilter=0, posted=0, postedOffsets=NONE)",
            workSessionStatistics.toString()
        )
        assertTrue(Prometheus.metricsAsText.contains("consumed 0.0"))

        workSessionStatistics.incConsumed(1)
        workSessionStatistics.incBlockedByFilter(2)

        // Updated state is reflected in .toString() and in metrics
        assertEquals(
            "WorkSessionStatistics(consumed=1, blockedByFilter=2, posted=0, postedOffsets=NONE)",
            workSessionStatistics.toString()
        )
        assertTrue(Prometheus.metricsAsText.contains("consumed 1.0"))
        assertTrue(Prometheus.metricsAsText.contains("blocked_by_filter 2.0"))

        // New work session resets data class before update, but not metrics
        workSessionStatistics = WorkSessionStatistics()
        workSessionStatistics.incConsumed(10)
        assertEquals(
            "WorkSessionStatistics(consumed=10, blockedByFilter=0, posted=0, postedOffsets=NONE)",
            workSessionStatistics.toString()
        )
        assertTrue(Prometheus.metricsAsText.contains("consumed 11.0")) // Total during lifetime
    }

    @Test
    fun `Test that posted work sessions statistics and lifetime metrics are updated as expected`() {
        var workSessionStatistics = WorkSessionStatistics()

        // Initial state is reflected in .toString() and in metrics
        assertEquals(
            "WorkSessionStatistics(consumed=0, blockedByFilter=0, posted=0, postedOffsets=NONE)",
            workSessionStatistics.toString()
        )
        assertTrue(Prometheus.metricsAsText.contains("posted 0.0"))

        val list = listOf(
            ConsumerRecord("topic", 0, 1L, "key", "value"),
            ConsumerRecord("topic", 0, 2L, "key", "value"),
            ConsumerRecord("topic", 0, 3L, "key", "value"),
            ConsumerRecord("topic", 1, 10L, "key", "value"),
            ConsumerRecord("topic", 1, 11L, "key", "value")
        )

        workSessionStatistics.updatePostedStatistics(list)

        assertTrue(Prometheus.metricsAsText.contains("latest_posted_offset{partition=\"0\",} 3.0"))
        assertTrue(Prometheus.metricsAsText.contains("latest_posted_offset{partition=\"1\",} 11.0"))

        assertEquals(
            "WorkSessionStatistics(consumed=0, blockedByFilter=0, posted=5, postedOffsets=0:[1-3],1:[10-11])",
            workSessionStatistics.toString()
        )
    }
}
