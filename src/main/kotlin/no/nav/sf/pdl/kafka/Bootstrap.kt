package no.nav.sf.pdl.kafka

val devContext = env(config_CONTEXT) == "DEV"

val application = when (env(config_DEPLOY_APP)) {
    "sf-pdl-kafka" -> KafkaPosterApplication(
        filter = if (devContext) ::isTombstoneOrSalesforceTaggedMod else null,
        modifier = ::reduceByWhitelist
    )
    else -> KafkaPosterApplication()
}

fun main() = application.start()
