package no.nav.sf.pdl.kafka.kafka

import no.nav.sf.pdl.kafka.config_FLAG_ALT_ID
import no.nav.sf.pdl.kafka.config_KAFKA_CLIENT_ID
import no.nav.sf.pdl.kafka.env
import no.nav.sf.pdl.kafka.envAsBoolean
import no.nav.sf.pdl.kafka.env_KAFKA_BROKERS
import no.nav.sf.pdl.kafka.env_KAFKA_CREDSTORE_PASSWORD
import no.nav.sf.pdl.kafka.env_KAFKA_KEYSTORE_PATH
import no.nav.sf.pdl.kafka.env_KAFKA_TRUSTSTORE_PATH
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties

// Instantiate each get() to fetch config from current state of environment (fetch injected updates of credentials)
private val propertiesBase get() = Properties().apply {
    val clientId = env(config_KAFKA_CLIENT_ID) + (if (envAsBoolean(config_FLAG_ALT_ID)) "-alt" else "")
    putAll(
        mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to env(env_KAFKA_BROKERS),
            ConsumerConfig.GROUP_ID_CONFIG to clientId,
            ConsumerConfig.CLIENT_ID_CONFIG to clientId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 200,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
            SaslConfigs.SASL_MECHANISM to "PLAIN",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to env(env_KAFKA_KEYSTORE_PATH),
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to env(env_KAFKA_CREDSTORE_PASSWORD),
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to env(env_KAFKA_TRUSTSTORE_PATH),
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to env(env_KAFKA_CREDSTORE_PASSWORD)
        )
    )
}

// Instantiate each get() to fetch config from current state of environment (fetch injected updates of credentials)
val propertiesPlain get() = propertiesBase.apply {
    putAll(
        mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
        )
    )
}
