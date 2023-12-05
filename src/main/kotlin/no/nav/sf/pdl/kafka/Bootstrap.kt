package no.nav.sf.pdl.kafka

val application = KafkaPosterApplication<String, String>(
    envAsSettings(env_POSTER_SETTINGS),
    ::reduceByWhitelist,
    if (devContext) ::filterOnSalesforceTag else null
)

fun main() = application.start()
