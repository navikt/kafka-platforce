package no.nav.sf.pdl.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IsTombstoneOrSalesforceTaggedTest {
    private val exampleWithoutTagElement = readResourceFile("/exampleWithoutTagElement.json").asRecordValue()
    private val exampleWithoutTag = readResourceFile("/exampleWithoutTag.json").asRecordValue()
    private val exampleWithoutSalesforceTag = readResourceFile("/exampleWithoutSalesforceTag.json").asRecordValue()
    private val exampleWithSalesforceTag = readResourceFile("/exampleWithSalesforceTag.json").asRecordValue()
    private val exampleTombstone = null.asRecordValue()

    private fun String?.asRecordValue() = ConsumerRecord("topic", 0, 0L, "key", this)

    @Test
    fun should_only_allow_message_with_salesforce_tag_or_tombstone() {
        assertEquals(false, isTombstoneOrSalesforceTagged(exampleWithoutTagElement))
        assertEquals(false, isTombstoneOrSalesforceTagged(exampleWithoutTag))
        assertEquals(false, isTombstoneOrSalesforceTagged(exampleWithoutSalesforceTag))

        assertEquals(true, isTombstoneOrSalesforceTagged(exampleWithSalesforceTag))
        assertEquals(true, isTombstoneOrSalesforceTagged(exampleTombstone))
    }
}
