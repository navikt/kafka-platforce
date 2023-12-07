package no.nav.sf.pdl.kafka

import isTombstoneOrSalesforceTagged
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IsTombstoneOrSalesforceTaggedTest {
    val exampleWithoutTagElement = readResourceFile("/exampleWithoutTagElement.json")
    val exampleWithoutTag = readResourceFile("/exampleWithoutTag.json")
    val exampleWithoutSalesforceTag = readResourceFile("/exampleWithoutSalesforceTag.json")
    val exampleWithSalesforceTag = readResourceFile("/exampleWithSalesforceTag.json")

    @Test
    fun should_only_allow_message_with_salesforce_tag_or_tombstone() {
        assertEquals(false, isTombstoneOrSalesforceTagged(exampleWithoutTagElement, 0L))
        assertEquals(false, isTombstoneOrSalesforceTagged(exampleWithoutTag, 0L))
        assertEquals(false, isTombstoneOrSalesforceTagged(exampleWithoutSalesforceTag, 0L))

        assertEquals(true, isTombstoneOrSalesforceTagged(exampleWithSalesforceTag, 0L))
        assertEquals(true, isTombstoneOrSalesforceTagged("null", 0L))
    }
}
