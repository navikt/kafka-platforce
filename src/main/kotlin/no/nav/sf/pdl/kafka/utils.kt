package no.nav.sf.pdl.kafka

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.JsonPrimitive
import io.prometheus.client.Histogram
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.sf.pdl.kafka.nais.PrestopHook
import no.nav.sf.pdl.kafka.nais.ShutdownHook
import org.apache.http.HttpHost
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClients
import org.http4k.client.ApacheClient
import org.http4k.core.HttpHandler
import org.http4k.core.Request
import org.http4k.core.Response
import java.io.File
import java.net.URI
import kotlin.streams.toList

private val log = KotlinLogging.logger { }

val gson = Gson()

val devContext = envOrNull(env_CONTEXT) == "DEV"

fun ApacheClient.supportProxy(httpsProxy: String): HttpHandler = httpsProxy.let { p ->
    when {
        p.isEmpty() -> this()
        else -> {
            val up = URI(p)
            this(
                client =
                    HttpClients.custom()
                        .setDefaultRequestConfig(
                            RequestConfig.custom()
                                .setProxy(HttpHost(up.host, up.port, up.scheme))
                                .setRedirectsEnabled(false)
                                .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                                .build()
                        )
                        .build()
            )
        }
    }
}

fun HttpHandler.measure(r: Request, m: Histogram): Response =
    m.startTimer().let { rt ->
        this(r).also {
            rt.observeDuration() // Histogram will store response time
            File("/tmp/lastTokenCall").writeText("uri: ${r.uri}, method: ${r.method}, body: ${r.body}, headers ${r.headers}")
        }
    }

fun ByteArray.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this)
fun String.encodeB64UrlSafe(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this.toByteArray())
fun String.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64String(this.toByteArray())
fun String.decodeB64(): ByteArray = org.apache.commons.codec.binary.Base64.decodeBase64(this)

/**
 * conditionalWait
 * Interruptable wait function
 */
fun conditionalWait(ms: Long) =
    runBlocking {

        log.info { "Will wait $ms ms" }

        val cr = launch {
            runCatching { delay(ms) }
                .onSuccess { log.info { "waiting completed" } }
                .onFailure { log.info { "waiting interrupted" } }
        }

        tailrec suspend fun loop(): Unit = when {
            cr.isCompleted -> Unit
            ShutdownHook.isActive() || PrestopHook.isActive() -> cr.cancel()
            else -> {
                delay(250L)
                loop()
            }
        }
        loop()
        cr.join()
    }

/**
 * offsetMapsToText
 * Create a string to represent the spans of offsets that has been posted
 * Example: 0:[12034-16035],1:[11240-15273]
 */
fun offsetMapsToText(firstOffset: MutableMap<Int, Long>, lastOffset: MutableMap<Int, Long>): String {
    if (firstOffset.isEmpty()) return "NONE"
    return firstOffset.keys.sorted().map {
        "$it:[${firstOffset[it]}-${lastOffset[it]}]"
    }.joinToString(",")
}

fun filterOnSalesforceTag(input: String, offset: Long): Boolean {
    try {
        val obj = JsonParser.parseString(input) as JsonObject
        if (obj["tags"] == null || obj["tags"] is JsonNull) return false
        return (obj["tags"] as JsonArray).any { it.asString == "SALESFORCE" }
    } catch (e: Exception) {
        File("/tmp/filtercontainssalesforcetagfail").appendText("OFFSET $offset\n${input}\n\n${e.message}")
        throw RuntimeException("Unable to parse input for salesforce tag filter ${e.message}")
    }
}

fun reduceByWhitelist(
    input: String,
    offset: Long,
    whitelist: String =
        KafkaPosterApplication::class.java.getResource(env("WHITELIST_FILE")).readText()
): String {
    try {
        val whitelistObject = JsonParser.parseString(whitelist) as JsonObject
        val messageObject = JsonParser.parseString(input) as JsonObject

        val result: MutableList<List<String>> = mutableListOf()
        findNonWhitelistedFields(whitelistObject, messageObject, result)

        result.forEach {
            // println("Will attempt remove of $it")
            messageObject.removeFieldRecurse(it)
            // println("Status: $messageObject")
        }

        return messageObject.toString()
    } catch (e: Exception) {
        File("/tmp/reducebywhitelistfail").appendText("OFFSET $offset\n${input}\n\n")
        throw RuntimeException("Unable to parse event and filter to reduce by whitelist")
    }
}

fun findNonWhitelistedFields(
    whitelistNode: JsonElement,
    messageNode: JsonElement,
    resultHolder: MutableList<List<String>>,
    parents: List<String> = listOf()
) {
    val whitelistEntrySet = (whitelistNode as JsonObject).entrySet()

    val messageEntrySet = if (messageNode is JsonArray) {
        messageNode.map { it as JsonObject }.flatMap { it.entrySet() }
    } else { (messageNode as JsonObject).entrySet() }

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
}

fun JsonElement.removeFieldRecurse(fieldTree: List<String>) {
    if (fieldTree.size == 1) {
        // println("remove ${fieldTree.first()}")
        if (this is JsonObject) {
            this.remove(fieldTree.first())
        } else if (this is JsonArray) {
            this.forEach {
                (it as JsonObject).remove(fieldTree.first())
            }
        } else {
            throw IllegalStateException("JsonElement.removeFieldRecurse attempted removing on primitive or null")
        }
    } else {
        if (this is JsonObject) {
            this.get(fieldTree.first()).removeFieldRecurse(fieldTree.subList(1, fieldTree.size))
        } else if (this is JsonArray) {
            this.forEach {
                (it as JsonObject).get(fieldTree.first()).removeFieldRecurse(fieldTree.subList(1, fieldTree.size))
            }
        } else {
            throw IllegalStateException("JsonElement.removeFieldRecurse attempted stepping into on primitive or null")
        }
    }
}

fun readResourceFile(path: String) = KafkaPosterApplication::class.java.getResource(path).readText()

/**
 * Shortcuts for fetching environment variables
 */
fun env(env: String): String { return System.getenv(env) }

fun envOrNull(env: String): String? { return System.getenv(env) }

fun envAsLong(env: String): Long { return System.getenv(env).toLong() }

fun envAsInt(env: String): Int { return System.getenv(env).toInt() }

fun envAsList(env: String): List<String> { return System.getenv(env).split(",").map { it.trim() }.toList() }

fun envAsSettings(env: String): List<KafkaToSFPoster.Settings> { return envAsList(env).stream().map { KafkaToSFPoster.Settings.valueOf(it) }.toList() }
