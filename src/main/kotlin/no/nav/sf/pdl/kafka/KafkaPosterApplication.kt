package no.nav.sf.pdl.kafka

import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging
import no.nav.sf.pdl.kafka.metrics.WorkSessionStatistics
import no.nav.sf.pdl.kafka.nais.ShutdownHook
import no.nav.sf.pdl.kafka.nais.naisAPI
import no.nav.sf.pdl.kafka.poster.KafkaToSFPoster
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.http4k.server.ApacheServer
import org.http4k.server.asServer

/**
 * KafkaPosterApplication
 * This is the top level of the integration. Its function is to setup a server with the required
 * endpoints for the kubernetes environement
 * and create a work loop that alternatives between work sessions (i.e polling from kafka until we are in sync) and
 * an interruptable pause (configured with MS_BETWEEN_WORK).
 */
class KafkaPosterApplication(
    filter: ((ConsumerRecord<String, String?>) -> Boolean)? = null,
    modifier: ((ConsumerRecord<String, String?>) -> String?)? = null,
) {
    private val poster = KafkaToSFPoster(filter, modifier)

    private val msBetweenWork = envAsLong(env_MS_BETWEEN_WORK + "_donotexist")

    private val log = KotlinLogging.logger { }

    fun start() {
        log.info { "Starting app ${envOrNull(env_DEPLOY_APP)} - devContext $devContext with poster settings ${envAsFlags(env_POSTER_FLAGS)}" }
        DefaultExports.initialize() // Instantiate Prometheus standard metrics
        naisAPI().asServer(ApacheServer(8080)).start()

        while (!ShutdownHook.isActive()) {
            try {
                poster.runWorkSession()
            } catch (e: Exception) {
                log.error { "A work session failed:\n${e.stackTraceToString()}" }
                WorkSessionStatistics.workSessionExceptionCounter.inc()
            }
            conditionalWait(msBetweenWork)
        }
    }
}
