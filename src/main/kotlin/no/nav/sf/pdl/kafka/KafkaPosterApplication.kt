package no.nav.sf.pdl.kafka

import io.prometheus.client.hotspot.DefaultExports
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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
 * This is the top level of the integration. Its function is to set up a server with the required
 * endpoints for the kubernetes environment
 * and create a work loop that alternatives between work sessions (i.e polling from kafka until we are in sync) and
 * an interruptable pause (configured with MS_BETWEEN_WORK).
 */
class KafkaPosterApplication(
    filter: ((ConsumerRecord<String, String?>) -> Boolean)? = null,
    modifier: ((ConsumerRecord<String, String?>) -> String?)? = null,
) {
    private val poster = KafkaToSFPoster(filter, modifier)

    private val msBetweenWork = envAsLong(config_MS_BETWEEN_WORK)

    private val log = KotlinLogging.logger { }

    fun start() {
        log.info {
            "Starting app ${env(config_DEPLOY_APP)} - devContext $devContext" +
                (if (envAsBoolean(config_FLAG_SEEK)) " - SEEK ${envAsLong(config_SEEK_OFFSET)}" else "") +
                (if (envAsBoolean(config_FLAG_SAMPLE)) " - SAMPLE" else "") +
                (if (envAsBoolean(config_FLAG_NO_POST)) " - NO_POST" else "") +
                (if (envAsBoolean(config_FLAG_RUN_ONCE)) " - RUN_ONCE" else "") +
                (if (envAsBoolean(config_FLAG_ALT_ID)) " - ALT_ID" else "")
        }
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

    /**
     * conditionalWait
     * Interruptable wait function
     */
    private fun conditionalWait(ms: Long) =
        runBlocking {
            log.debug { "Will wait $ms ms" }

            val waitJob = launch {
                runCatching { delay(ms) }
                    .onSuccess { log.info { "waiting completed" } }
                    .onFailure { log.info { "waiting interrupted" } }
            }

            tailrec suspend fun loop(): Unit = when {
                waitJob.isCompleted -> Unit
                ShutdownHook.isActive() -> waitJob.cancel()
                else -> {
                    delay(250L)
                    loop()
                }
            }
            loop()
            waitJob.join()
        }
}
