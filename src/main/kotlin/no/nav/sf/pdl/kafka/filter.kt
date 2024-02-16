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
        File("/tmp/filtercontainssalesforcetagfail").appendText("$record\nMESSAGE ${e.message}\n\n")
        throw RuntimeException("Unable to parse value for salesforce tag filter ${e.message}")
    }
}
