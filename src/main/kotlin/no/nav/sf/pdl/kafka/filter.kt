package no.nav.sf.pdl.kafka

import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File

val devPersonsKeysOffsets: MutableMap<String, Long> = mutableMapOf()

fun isTombstoneOrSalesforceTagged(record: ConsumerRecord<String, String?>): Boolean {
    try {
        if (record.value() == null) return true.also { updatePassedFile(record) } // Allow tombstone signal
        val obj = JsonParser.parseString(record.value()) as JsonObject
        if (obj["tags"] == null || obj["tags"] is JsonNull) return false
        return (obj["tags"] as JsonArray).any { it.asString == "SALESFORCE" }.also {
            if (it) {
                updatePassedFile(record)
            }
        }
    } catch (e: Exception) {
        File("/tmp/filterSalesforceTagFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for salesforce tag filter ${e.message}")
    }
}

fun updatePassedFile(record: ConsumerRecord<String, String?>) {
    val tombstone = record.value() == null
    val latestRefOffset = devPersonsKeysOffsets[record.key()]?.toString() ?: ""
    File("/tmp/passed").appendText("${record.offset()} ${if (tombstone) "TOMBSTONE" else "PERSON"} LATEST $latestRefOffset\n")
    if (!tombstone) devPersonsKeysOffsets[record.key()] = record.offset()
}
