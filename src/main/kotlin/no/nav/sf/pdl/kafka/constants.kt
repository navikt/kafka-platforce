package no.nav.sf.pdl.kafka

// Config environment variables set in yaml file
const val config_DEPLOY_APP = "DEPLOY_APP"
const val config_POSTER_FLAGS = "POSTER_FLAGS"
const val config_MS_BETWEEN_WORK = "MS_BETWEEN_WORK"
const val config_KAFKA_CLIENTID = "KAFKA_CLIENTID"
const val config_KAFKA_TOPIC = "KAFKA_TOPIC"
const val config_KAFKA_POLL_DURATION = "KAFKA_POLL_DURATION"
const val config_WHITELIST_FILE = "WHITELIST_FILE"
const val config_CONTEXT = "CONTEXT"

// Kafka injected environment dependencies
const val env_KAFKA_BROKERS = "KAFKA_BROKERS"
const val env_KAFKA_KEYSTORE_PATH = "KAFKA_KEYSTORE_PATH"
const val env_KAFKA_CREDSTORE_PASSWORD = "KAFKA_CREDSTORE_PASSWORD"
const val env_KAFKA_TRUSTSTORE_PATH = "KAFKA_TRUSTSTORE_PATH"

// Salesforce injected environment dependencies
const val env_SF_TOKENHOST = "SF_TOKENHOST"

// Salesforce required secrets
const val secret_SFClientID = "SFClientID"
const val secret_SFUsername = "SFUsername"

// Salesforce required secrets related to keystore for signed JWT
const val secret_keystoreJKSB64 = "keystoreJKSB64"
const val secret_KeystorePassword = "KeystorePassword"
const val secret_PrivateKeyAlias = "PrivateKeyAlias"
const val secret_PrivateKeyPassword = "PrivateKeyPassword"

const val NUMBER_OF_SAMPLES_IN_SAMPLE_RUN = 3
const val SALESFORCE_VERSION = "v57.0"
