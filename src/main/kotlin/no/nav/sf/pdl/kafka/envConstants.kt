package no.nav.sf.pdl.kafka

import no.nav.sf.pdl.kafka.poster.KafkaToSFPoster

/**
 * Naming convention applied to environment variable constants: a lowercase prefix separated from the actual constant, i.e. prefix_ENVIRONMENT_VARIABLE_NAME.
 *
 * Motivation:
 * The prefix provides contextual naming that describes the source and nature of the variables they represent while keeping the names short.
 * A prefix marks a constant representing an environment variable, and also where one can find the value of that variable
 *
 * - env: Denotes an environment variable typically injected into the pod by the Nais platform.
 *
 * - config: Denotes an environment variable explicitly configured in YAML files (see dev.yaml, prod.yaml)
 *
 * - secret: Denotes an environment variable loaded from a Kubernetes secret.
 */

// Config environment variables set in yaml file
const val config_DEPLOY_APP = "DEPLOY_APP"
const val config_POSTER_FLAGS = "POSTER_FLAGS"
const val config_SEEK_OFFSET = "SEEK_OFFSET"
const val config_MS_BETWEEN_WORK = "MS_BETWEEN_WORK"
const val config_KAFKA_CLIENT_ID = "KAFKA_CLIENT_ID"
const val config_KAFKA_TOPIC = "KAFKA_TOPIC"
const val config_KAFKA_POLL_DURATION = "KAFKA_POLL_DURATION"
const val config_WHITELIST_FILE = "WHITELIST_FILE"
const val config_CONTEXT = "CONTEXT"

// Kafka injected environment dependencies
const val env_KAFKA_BROKERS = "KAFKA_BROKERS"
const val env_KAFKA_KEYSTORE_PATH = "KAFKA_KEYSTORE_PATH"
const val env_KAFKA_CREDSTORE_PASSWORD = "KAFKA_CREDSTORE_PASSWORD"
const val env_KAFKA_TRUSTSTORE_PATH = "KAFKA_TRUSTSTORE_PATH"

// Salesforce configured environment dependency
const val config_SF_TOKENHOST = "SF_TOKENHOST"

// Salesforce required secrets
const val secret_SF_CLIENT_ID = "SF_CLIENT_ID"
const val secret_SF_USERNAME = "SF_USERNAME"

// Salesforce required secrets related to keystore for signed JWT
const val secret_KEYSTORE_JKS_B64 = "KEYSTORE_JKS_B64"
const val secret_KEYSTORE_PASSWORD = "KEYSTORE_PASSWORD"
const val secret_PRIVATE_KEY_ALIAS = "PRIVATE_KEY_ALIAS"
const val secret_PRIVATE_KEY_PASSWORD = "PRIVATE_KEY_PASSWORD"

/**
 * Shortcuts for fetching environment variables
 */
fun env(name: String): String { return System.getenv(name) }

fun envAsLong(name: String): Long { return System.getenv(name).toLong() }

fun envAsList(name: String): List<String> { return System.getenv(name).split(",").map { it.trim() }.toList() }

fun envAsFlags(name: String): List<KafkaToSFPoster.Flag> { return envAsList(name).stream().map { KafkaToSFPoster.Flag.valueOf(it) }.toList() }
