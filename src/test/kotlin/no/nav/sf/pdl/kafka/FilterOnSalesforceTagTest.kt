package no.nav.sf.pdl.kafka

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class FilterOnSalesforceTagTest {
    val exampleWithoutTagElement = readResourceFile("/exampleWithoutTagElement.json")
    val exampleWithoutTag = readResourceFile("/exampleWithoutTag.json")
    val exampleWithoutSalesforceTag = readResourceFile("/exampleWithoutSalesforceTag.json")
    val exampleWithSalesforceTag = readResourceFile("/exampleWithSalesforceTag.json")

    @Test
    fun should_only_allow_message_with_salesforce_tag() {
        assertEquals(false, filterOnSalesforceTag(exampleWithoutTagElement, 0L))
        assertEquals(false, filterOnSalesforceTag(exampleWithoutTag, 0L))
        assertEquals(false, filterOnSalesforceTag(exampleWithoutSalesforceTag, 0L))

        assertEquals(true, filterOnSalesforceTag(exampleWithSalesforceTag, 0L))
    }
}
