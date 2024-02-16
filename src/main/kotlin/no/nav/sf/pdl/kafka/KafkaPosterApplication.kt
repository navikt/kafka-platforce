package no.nav.sf.pdl.kafka

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.nais.ShutdownHook
import no.nav.sf.pdl.kafka.nais.naisAPI
import no.nav.sf.pdl.kafka.poster.KafkaToSFPoster
import nullNull
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.http4k.server.ApacheServer
import org.http4k.server.asServer
import stringNull

/**
 * KafkaPosterApplication
 * This is the top level of the integration. Its function is to setup a server with the required
 * endpoints for the kubernetes environement
 * and create a work loop that alternatives between work sessions (i.e polling from kafka until we are in sync) and
 * an interruptable pause (configured with MS_BETWEEN_WORK).
 */
class KafkaPosterApplication<K, V>(
    filter: ((ConsumerRecord<String, String>) -> Boolean)? = null,
    modifier: ((ConsumerRecord<String, String>) -> String)? = null
) {
    val poster = KafkaToSFPoster<K, V>(modifier, filter)
    val posterPlain = KafkaToSFPoster<K, V>()

    private val bootstrapWaitTime = envAsLong(env_MS_BETWEEN_WORK)

    private val log = KotlinLogging.logger { }

    fun start() {
        log.info { "Starting app ${envOrNull(env_DEPLOY_APP)} - devContext $devContext with poster settings ${envAsSettings(env_POSTER_SETTINGS)}" }
        naisAPI().asServer(ApacheServer(8080)).start()
        loop()
    }

    private tailrec fun loop() {
        val stop = ShutdownHook.isActive()
        when {
            stop -> Unit.also { log.info { "Stopped" } }
            else -> {
                try {
                    poster.runWorkSession(env(env_KAFKA_TOPIC_PERSONDOKUMENT))
                    // posterPlain.runWorkSession(env(env_KAFKA_TOPIC_GEOGRAFISKTILKNYTNING))
                } catch (e: Exception) {
                    log.error { "A work session failed:\n${e.stackTraceToString()}" }
                }
                log.info { "End of work session. Tombstone stats - string $stringNull, null $nullNull" }
                conditionalWait(bootstrapWaitTime)
                loop()
            }
        }
    }
}
