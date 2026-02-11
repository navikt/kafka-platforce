package no.nav.sf.pdl.kafka.nais

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.application
import no.nav.sf.pdl.kafka.gui.Gui
import no.nav.sf.pdl.kafka.investigate.Investigate
import no.nav.sf.pdl.kafka.metrics.Prometheus
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.core.Status.Companion.OK
import org.http4k.routing.bind
import org.http4k.routing.routes

private val log = KotlinLogging.logger { }

fun naisAPI(): HttpHandler =
    routes(
        "/internal/isAlive" bind Method.GET to { Response(Status.OK) },
        "/internal/isReady" bind Method.GET to { Response(Status.OK) },
        "/internal/metrics" bind Method.GET to {
            try {
                val result = Prometheus.metricsAsText
                if (result.isEmpty()) {
                    Response(Status.NO_CONTENT)
                } else {
                    Response(Status.OK).body(result)
                }
            } catch (e: Exception) {
                log.error { "/prometheus failed writing metrics -  ${e.message}" }
                Response(Status.INTERNAL_SERVER_ERROR)
            }
        },
        "/internal/gui" bind Method.GET to Gui.guiHandler,
        "/internal/investigate" bind Method.GET to Investigate.investigateHandler,
        "/internal/clearDb" bind Method.GET to clearDbHandler,
        "/internal/initDb" bind Method.GET to initDbHandler,
    )

object ShutdownHook {
    private val log = KotlinLogging.logger { }

    @Volatile
    private var shutdownhookActive = false
    private val mainThread: Thread = Thread.currentThread()

    init {
        log.info { "Installing shutdown hook" }
        Runtime
            .getRuntime()
            .addShutdownHook(
                object : Thread() {
                    override fun run() {
                        shutdownhookActive = true
                        log.info { "shutdown hook activated" }

                        mainThread.join()
                    }
                },
            )
    }

    fun isActive() = shutdownhookActive
}

private val clearDbHandler: HttpHandler = {
    if (application.db != null) {
        application.db!!.createKafkaOffsetTable(true)
        Response(OK).body("Table recreated")
    } else {
        Response(OK).body("There is no active database connection")
    }
}

private val initDbHandler: HttpHandler = {
    if (application.db != null) {
        application.db!!.createKafkaOffsetTable(false)
        Response(OK).body("Table created")
    } else {
        Response(OK).body("There is no active database connection")
    }
}
