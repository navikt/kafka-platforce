package no.nav.sf.pdl.kafka.salesforce

import mu.KotlinLogging

class MigratingAccessTokenHandler(
    val old: DefaultAccessTokenHandler,
    val new: NewAccessTokenHandler,
) : AccessTokenHandler {
    private val log = KotlinLogging.logger {}

    override val accessToken: String
        get() {
            return try {
                val token = new.accessToken
                log.info { "Retrieved access token via new" }
                token
            } catch (e: Throwable) {
                log.warn(e) { "New token handler failed, falling back to old" }
                val token = old.accessToken
                log.info { "Retrieved access token via old" }
                token
            }
        }
    override val instanceUrl: String
        get() {
            return try {
                val token = new.instanceUrl
                log.info { "Retrieved instance url via new" }
                token
            } catch (e: Throwable) {
                log.warn(e) { "New token handler failed, falling back to old" }
                val token = old.instanceUrl
                log.info { "Retrieved instance url via old" }
                token
            }
        }
    override val tenantId get() = "N/A"
}
