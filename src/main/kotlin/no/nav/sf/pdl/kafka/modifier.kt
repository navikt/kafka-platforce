package no.nav.sf.pdl.kafka

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.JsonPrimitive
import no.nav.sf.pdl.kafka.gui.Gui
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.File

fun reduceByWhitelistAndRemoveHistoricalItems(
    record: ConsumerRecord<String, String?>,
    whitelist: String =
        KafkaPosterApplication::class.java.getResource(env(config_WHITELIST_FILE)).readText()
): String? {
    if (record.value() == null) return null // Tombstone - no reduction to be made
    try {
        val whitelistObject = JsonParser.parseString(whitelist) as JsonObject
        val messageObject = JsonParser.parseString(record.value()) as JsonObject

        val removeList = findNonWhitelistedFields(whitelistObject, messageObject)

        File("/tmp/latestDroppedPdlFields").writeText(
            removeList.map { it.joinToString(".") }.joinToString("\n")
        )

        val allList = listAllFields(messageObject)

        Gui.latestMessageAndRemovalSets = Pair(allList, removeList)

        removeList.forEach {
            messageObject.removeFields(it)
        }

        return removeHistoricalItems(messageObject.toString())
    } catch (e: Exception) {
        File("/tmp/reducebywhitelistfail").appendText("$record\n\n")
        throw RuntimeException("Unable to parse event and filter to reduce by whitelist")
    }
}

/**
 * findNonWhitelistedField
 * A function designed to identify and collect paths of non-whitelisted fields within a
 * JSON structure, based on a specified whitelist
 *
 * - whitelistElement: A JsonElement representing the whitelist JSON structure.
 *                     It serves as the reference for determining which fields are allowed.
 * - messageElement: A JsonElement representing the JSON structure to be analyzed.
 */
private fun findNonWhitelistedFields(
    whitelistElement: JsonElement,
    messageElement: JsonElement,
    resultHolder: MutableSet<List<String>> = mutableSetOf(),
    parents: List<String> = listOf(),
): Set<List<String>> {
    val whitelistEntrySet = (whitelistElement as JsonObject).entrySet()

    val messageEntrySet = if (messageElement is JsonArray) {
        messageElement.map { it as JsonObject }.flatMap { it.entrySet() }.toSet()
    } else { (messageElement as JsonObject).entrySet() }

    // Whitelist field with primitive value (typically "ALL") means allow field plus any subfields
    val whitelistPrimitives = whitelistEntrySet.filter { it.value is JsonPrimitive }.map { it.key }.toList()

    // Whitelist fields that contains another json object, means allow top field and the subobject will
    // describe what parts to allow for any subfields
    val whitelistObjects = whitelistEntrySet.filter { it.value is JsonObject }.map { it.key }.toList()

    val removeList = messageEntrySet.filter { entry ->
        // Never remove if fields is whitelisted as "ALL"
        if (whitelistPrimitives.contains(entry.key)) return@filter false

        // If not whitelisted as "ALL", remove any primitives and null
        if (entry.value is JsonPrimitive || entry.value is JsonNull) return@filter true

        // If field is object or array, only keep it if member of object whitelist
        !whitelistObjects.contains(entry.key)
    }.map { parents + it.key }

    resultHolder.addAll(removeList)

    // Apply recursively on any whitelist subnodes, given that the message node has corresponding array or object subnode
    whitelistEntrySet
        .filter { it.value is JsonObject }
        .forEach { whitelistEntry ->
            messageEntrySet
                .firstOrNull { it.key == whitelistEntry.key && (it.value is JsonObject || it.value is JsonArray) }
                ?.let { messageEntry ->
                    findNonWhitelistedFields(
                        whitelistEntry.value,
                        messageEntry.value,
                        resultHolder,
                        parents.toList() + whitelistEntry.key,
                    )
                }
        }
    return resultHolder
}

/**
 * JsonElement.removeField
 * Recursive extension function that facilitates the removal of a specified field within a JSON structure
 * It supports recursive removal, allowing the removal of nested fields.
 *
 * - fieldTree: A list of strings representing the path to the field to be removed.
 */
private fun JsonElement.removeFields(fieldTree: List<String>) {
    if (fieldTree.size == 1) {
        when (this) {
            is JsonObject -> this.remove(fieldTree.first())
            is JsonArray -> this.forEach {
                (it as JsonObject).remove(fieldTree.first())
            }
            else -> {
                throw IllegalStateException("JsonElement.removeFieldRecurse attempted removing a primitive or null")
            }
        }
    } else {
        when (this) {
            is JsonObject -> this.get(fieldTree.first()).removeFields(fieldTree.subList(1, fieldTree.size))
            is JsonArray -> this.forEach {
                (it as JsonObject).get(fieldTree.first()).removeFields(fieldTree.subList(1, fieldTree.size))
            }
            else -> {
                throw IllegalStateException("JsonElement.removeFieldRecurse attempted stepping into a primitive or null")
            }
        }
    }
}

/**
 * listAllFields
 * Recursive function that traverses a json object a returns a set of field paths (flat description of all fields).
 */
private fun listAllFields(
    messageElement: JsonElement,
    resultHolder: MutableSet<List<String>> = mutableSetOf(),
    parents: List<String> = listOf()
): Set<List<String>> {
    val messageEntrySet = if (messageElement is JsonArray) {
        messageElement.filterIsInstance<JsonObject>().flatMap { it.entrySet() }.toSet()
    } else {
        (messageElement as JsonObject).entrySet()
    }

    val list = messageEntrySet.map { parents + it.key }

    resultHolder.addAll(list)

    messageEntrySet
        .filter { it.value is JsonObject || it.value is JsonArray }
        .forEach {
            listAllFields(
                it.value,
                resultHolder,
                parents.toList() + it.key
            )
        }
    return resultHolder
}

fun removeHistoricalItems(jsonString: String): String {
    // Parse the input JSON string
    val jsonElement: JsonElement = JsonParser.parseString(jsonString)
    val jsonObject: JsonObject = jsonElement.asJsonObject

    // Access the "hentPerson" object
    val hentPerson = jsonObject.getAsJsonObject("hentPerson")

    // Function to filter historical entries from an array
    fun filterHistoricalEntries(array: JsonArray): JsonArray {
        val updatedArray = JsonArray()

        for (element in array) {
            val itemObject = element.asJsonObject

            var isHistorical = false

            if (itemObject.has("metadata")) {
                val metadata = itemObject.getAsJsonObject("metadata")
                // Check if "historisk" is false or doesn't exist
                if (metadata.has("historisk")) {
                    isHistorical = metadata.get("historisk")?.asBoolean ?: false
                }
            }

            if (!isHistorical) {
                updatedArray.add(itemObject) // Keep this entry if not historical
            }
        }

        return updatedArray
    }

    // Check if "bostedsadresse" key exists and is an array, then filter
    if (hentPerson != null && hentPerson.has("bostedsadresse") && hentPerson.get("bostedsadresse").isJsonArray) {
        val bostedsadresseArray = hentPerson.getAsJsonArray("bostedsadresse")
        val filteredBostedsadresse = filterHistoricalEntries(bostedsadresseArray)
        hentPerson.add("bostedsadresse", filteredBostedsadresse)
    }

    // Check if "navn" key exists and is an array, then filter
    if (hentPerson != null && hentPerson.has("navn") && hentPerson.get("navn").isJsonArray) {
        val navnArray = hentPerson.getAsJsonArray("navn")
        val filteredNavn = filterHistoricalEntries(navnArray)
        hentPerson.add("navn", filteredNavn)
    }

    // Return the updated JSON as a string
    return jsonObject.toString()
}
