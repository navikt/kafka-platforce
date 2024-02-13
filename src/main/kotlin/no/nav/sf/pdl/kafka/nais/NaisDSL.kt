package no.nav.sf.pdl.kafka.nais

import io.prometheus.client.exporter.common.TextFormat
import mu.KotlinLogging
import no.nav.sf.pdl.kafka.metrics.Metrics.cRegistry
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.bind
import org.http4k.routing.routes
import java.io.StringWriter

private val log = KotlinLogging.logger { }

const val NAIS_URL = "http://localhost:"
const val NAIS_DEFAULT_PORT = 8080

fun naisAPI(): HttpHandler = routes(
    "/internal/isAlive" bind Method.GET to { Response(Status.OK) },
    "/internal/isReady" bind Method.GET to { Response(Status.OK) },
    "/internal/metrics" bind Method.GET to {
        runCatching {
            StringWriter().let { str ->
                TextFormat.write004(str, cRegistry.metricFamilySamples())
                str
            }.toString()
        }
            .onFailure {
                log.error { "/prometheus failed writing metrics - ${it.localizedMessage}" }
            }
            .getOrDefault("")
            .responseByContent()
    }
)

private fun String.responseByContent(): Response =
    if (this.isNotEmpty()) Response(Status.OK).body(this) else Response(Status.NO_CONTENT)

object ShutdownHook {
    private val log = KotlinLogging.logger { }

    @Volatile
    private var shutdownhookActive = false
    private val mainThread: Thread = Thread.currentThread()

    init {
        log.info { "Installing shutdown hook" }
        Runtime.getRuntime()
            .addShutdownHook(
                object : Thread() {
                    override fun run() {
                        shutdownhookActive = true
                        log.info { "shutdown hook activated" }
                        mainThread.join()
                    }
                })
    }

    fun isActive() = shutdownhookActive
}
