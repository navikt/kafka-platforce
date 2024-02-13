package no.nav.sf.pdl.kafka.salesforce

interface AccessTokenHandler {
    val accessToken: String
    val instanceUrl: String

    fun refreshToken()
}
