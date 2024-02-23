package no.nav.sf.pdl.kafka

import com.google.gson.Gson
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.sf.pdl.kafka.nais.ShutdownHook
import no.nav.sf.pdl.kafka.poster.KafkaToSFPoster
import java.util.Base64

private val log = KotlinLogging.logger { }

val gson = Gson()

val devContext = envOrNull(env_CONTEXT) == "DEV"

fun String.encodeB64(): String = Base64.getEncoder().encodeToString(this.toByteArray())

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

fun readResourceFile(path: String) = KafkaPosterApplication::class.java.getResource(path).readText()

/**
 * Shortcuts for fetching environment variables
 */
fun env(env: String): String { return System.getenv(env) }

fun envOrNull(env: String): String? { return System.getenv(env) }

fun envAsLong(env: String): Long { return System.getenv(env).toLong() }

fun envAsList(env: String): List<String> { return System.getenv(env).split(",").map { it.trim() }.toList() }

fun envAsFlags(env: String): List<KafkaToSFPoster.Flag> { return envAsList(env).stream().map { KafkaToSFPoster.Flag.valueOf(it) }.toList() }
