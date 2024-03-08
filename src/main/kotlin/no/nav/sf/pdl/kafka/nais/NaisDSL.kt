package no.nav.sf.pdl.kafka.nais

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import mu.KotlinLogging
import no.nav.sf.pdl.kafka.GuiRepresentation
import no.nav.sf.pdl.kafka.markRemovedFields
import no.nav.sf.pdl.kafka.metrics.Prometheus
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.bind
import org.http4k.routing.routes

private val log = KotlinLogging.logger { }

fun naisAPI(): HttpHandler = routes(
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
            log.error { "/prometheus failed writing metrics - ${e.message}" }
            Response(Status.INTERNAL_SERVER_ERROR)
        }
    },
    "/internal/gui" bind Method.GET to {
        GuiRepresentation.latestMerge = JsonObject()
        markRemovedFields(GuiRepresentation.latestMessageModel, GuiRepresentation.latestRemoval, GuiRepresentation.latestMerge)
        val prettifier = GsonBuilder().setPrettyPrinting().create()
        // .toString().replace(":{}", "")
        Response(Status.OK).body(generateHTML(prettifier.toJson(GuiRepresentation.latestMerge)))
    }
)

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

fun generateHTML(jsonString: String): String {
    val jsonLines = jsonString.split("\n")
    val htmlStringBuilder = StringBuilder()
    htmlStringBuilder.append("<html>")
    htmlStringBuilder.append("<head>")
    htmlStringBuilder.append("<style>")
    htmlStringBuilder.append(".toberemoved { background-color: #ffcccc; }")
    htmlStringBuilder.append("</style>")
    htmlStringBuilder.append("</head>")
    htmlStringBuilder.append("<body>")
    htmlStringBuilder.append("<pre>")
    for (line in jsonLines) {
        if (line.isNotBlank()) {
            val trimmedLine = line.trim()
            val spanClass = if (trimmedLine.startsWith("\"!")) " class=\"toberemoved\"" else ""
            htmlStringBuilder.append("<span$spanClass>${line.replace("!", "")}</span><br>")
        }
    }
    htmlStringBuilder.append("</pre>")
    htmlStringBuilder.append("</body>")
    htmlStringBuilder.append("</html>")
    return htmlStringBuilder.toString()
}
