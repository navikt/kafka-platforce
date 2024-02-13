package no.nav.sf.pdl.kafka

import com.google.gson.Gson
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.sf.pdl.kafka.nais.ShutdownHook
import kotlin.streams.toList

private val log = KotlinLogging.logger { }

val gson = Gson()

val devContext = envOrNull(env_CONTEXT) == "DEV"

fun String.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64String(this.toByteArray())

/**
 * conditionalWait
 * Interruptable wait function
 */
fun conditionalWait(ms: Long) =
    runBlocking {

        log.debug { "Will wait $ms ms" }

        val cr = launch {
            runCatching { delay(ms) }
                .onSuccess { log.info { "waiting completed" } }
                .onFailure { log.info { "waiting interrupted" } }
        }

        tailrec suspend fun loop(): Unit = when {
            cr.isCompleted -> Unit
            ShutdownHook.isActive() -> cr.cancel()
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
