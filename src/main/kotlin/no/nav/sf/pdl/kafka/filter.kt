import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import mu.KotlinLogging
import java.io.File

private val log = KotlinLogging.logger { }

fun isTombstoneOrSalesforceTagged(input: String, offset: Long): Boolean {
    try {
        if (input == "null") return true // Allow tombstone signal
        val obj = JsonParser.parseString(input) as JsonObject
        if (obj["tags"] == null || obj["tags"] is JsonNull) return false
        return (obj["tags"] as JsonArray).any { it.asString == "SALESFORCE" }
    } catch (e: Exception) {
        File("/tmp/filtercontainssalesforcetagfail").appendText("OFFSET $offset\nINPUT\n${input}\n\nMESSAGE ${e.message}\n")
        throw RuntimeException("Unable to parse input for salesforce tag filter ${e.message}")
    }
}
