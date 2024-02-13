package no.nav.sf.pdl.kafka.salesforce

import no.nav.sf.pdl.kafka.SALESFORCE_VERSION
import org.http4k.client.ApacheClient
import org.http4k.core.Headers
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import kotlin.system.measureTimeMillis

class SalesforceClient(
    private val httpClient: HttpHandler = ApacheClient(),
    private val accessTokenHandler: AccessTokenHandler = DefaultAccessTokenHandler()
) {
    operator fun invoke(body: String): Response {
        val dstUrl = "${accessTokenHandler.instanceUrl}/services/data/$SALESFORCE_VERSION/composite/sobjects"
        val headers: Headers =
            listOf(
                Pair("Authorization", "Bearer ${accessTokenHandler.accessToken}"),
            )
        val request = Request(Method.POST, dstUrl).headers(headers).body(body)
        lateinit var response: Response
        val ref =
            measureTimeMillis {
                response = httpClient(request)
            }
        return response
    }
}
