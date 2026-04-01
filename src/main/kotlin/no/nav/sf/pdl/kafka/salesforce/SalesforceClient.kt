package no.nav.sf.pdl.kafka.salesforce

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.config_DEPLOY_APP
import no.nav.sf.pdl.kafka.devContext
import no.nav.sf.pdl.kafka.env
import org.http4k.client.OkHttp
import org.http4k.core.Headers
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import java.io.File

const val SALESFORCE_VERSION = "v57.0"

private val log = KotlinLogging.logger { }

class SalesforceClient(
    private val httpClient: HttpHandler = OkHttp(),
    private val accessTokenHandler: AccessTokenHandler =
        if (devContext) {
            if (env(config_DEPLOY_APP) == "sf-pdl-kafka" || env(config_DEPLOY_APP) == "sf-geografisktilknytning") {
                log.info("Using new access token handler")
                NewAccessTokenHandler()
            } else {
                log.info("Using old access token handler")
                DefaultAccessTokenHandler()
            }
        } else {
            DefaultAccessTokenHandler()
        },
) {
    fun postRecords(kafkaMessages: Set<KafkaMessage>): Response {
        val requestBody = SFsObjectRest(records = kafkaMessages).toJson()

        val dstUrl = "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/composite/sobjects"

        val headers: Headers =
            listOf(
                "Authorization" to "Bearer ${accessTokenHandler.accessToken}",
                "Content-Type" to "application/json;charset=UTF-8",
            )

        val request = Request(Method.POST, dstUrl).headers(headers).body(requestBody)

        File("/tmp/latestPostRequest").writeText(request.toMessage())

        return httpClient(request)
    }
}
