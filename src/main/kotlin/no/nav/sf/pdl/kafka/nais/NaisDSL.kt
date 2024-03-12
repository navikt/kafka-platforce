package no.nav.sf.pdl.kafka.nais

import com.google.gson.JsonObject
import mu.KotlinLogging
import no.nav.sf.pdl.kafka.GuiRepresentation
import no.nav.sf.pdl.kafka.markRemovedFields
import no.nav.sf.pdl.kafka.metrics.Prometheus
import no.nav.sf.pdl.kafka.parseFieldsListToJsonObject
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
        val latestMessageModel = parseFieldsListToJsonObject(GuiRepresentation.latestMessageAndRemovalSets.first)
        val latestRemovalModel = parseFieldsListToJsonObject(GuiRepresentation.latestMessageAndRemovalSets.second)
        val latestMerge = JsonObject()
        markRemovedFields(latestMessageModel, latestRemovalModel, latestMerge)
        Response(Status.OK).body(generateHTMLFromJSON(GuiRepresentation.prettifier.toJson(latestMerge)))
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

fun generateHTMLFromJSON(jsonString: String): String {
    val jsonLines = jsonString.split("\n")
    return wrapWithHTMLPage(buildHTMLContentFromJsonLines(jsonLines))
}

private fun wrapWithHTMLPage(content: String): String {
    return """
        <html>
        <head>
        <style>
        .toberemoved { background-color: #ffcccc; }
        </style>
        </head>
        <body>
        $content
        </body>
        </html>
    """.trimIndent()
}

private fun buildHTMLContentFromJsonLines(jsonLines: List<String>): String {
    val htmlContent = StringBuilder()
    htmlContent.append("<pre>")
    for (line in jsonLines) {
        if (line.isNotBlank()) {
            val trimmedLine = line.trim()
            val spanClass = if (trimmedLine.startsWith("\"!")) " class=\"toberemoved\"" else ""
            val sanitizedLine = line.replace("!", "")
            htmlContent.append("<span$spanClass>$sanitizedLine</span><br>")
        }
    }
    htmlContent.append("</pre>")
    return htmlContent.toString()
}
