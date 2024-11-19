package no.nav.sf.pdl.kafka

import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File

fun isTombstoneOrSalesforceTagged(record: ConsumerRecord<String, String?>): Boolean {
    try {
        if (record.value() == null) return true // Allow tombstone signal
        val obj = JsonParser.parseString(record.value()) as JsonObject
        if (obj["tags"] == null || obj["tags"] is JsonNull) return false
        return (obj["tags"] as JsonArray).any { it.asString == "SALESFORCE" }
    } catch (e: Exception) {
        File("/tmp/filterSalesforceTagFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for salesforce tag filter ${e.message}")
    }
}

fun hasVergemaalEllerFremtidsfullmakt(record: ConsumerRecord<String, String?>): Boolean {
    try {
        if (record.value() == null) return true // Allow tombstone signal
        val obj = JsonParser.parseString(record.value()) as JsonObject
        if (obj["vergemaalEllerFremtidsfullmakt"] == null || obj["vergemaalEllerFremtidsfullmakt"] is JsonNull) return false
        if (obj["vergemaalEllerFremtidsfullmakt"] is JsonArray && (obj["vergemaalEllerFremtidsfullmakt"] as JsonArray).isEmpty) return false
        return true
    } catch (e: Exception) {
        File("/tmp/filterHasVergemaalEllerFremtidsfullmaktFail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for hasVergemaalEllerFremtidsfullmakt filter ${e.message}")
    }
}

val cherryPickList: List<Long> by lazy { KafkaPosterApplication::class.java.getResource("/offsetfile.txt").readText().split("\n").map { it.trim().toLong() } }

fun cherryPickOffsets(record: ConsumerRecord<String, String?>): Boolean {
    try {
        return cherryPickList.contains(record.offset())
    } catch (e: Exception) {
        File("/tmp/filterCherryPickFail").writeText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for cherry pick filter ${e.message}")
    }
}
