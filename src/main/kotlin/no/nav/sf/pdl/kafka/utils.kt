package no.nav.sf.pdl.kafka

import com.google.gson.Gson
import no.nav.sf.pdl.kafka.poster.KafkaToSFPoster

val gson = Gson()

val devContext = env(config_CONTEXT) == "DEV"

/**
 * Shortcuts for fetching environment variables
 */
fun env(name: String): String { return System.getenv(name) }

fun envAsLong(name: String): Long { return System.getenv(name).toLong() }

fun envAsList(name: String): List<String> { return System.getenv(name).split(",").map { it.trim() }.toList() }

fun envAsFlags(name: String): List<KafkaToSFPoster.Flag> { return envAsList(name).stream().map { KafkaToSFPoster.Flag.valueOf(it) }.toList() }
