package no.nav.sf.pdl.kafka

val application = when (env(config_DEPLOY_APP)) {
    "sf-pdl-kafka" -> KafkaPosterApplication(
        filter = if (devContext) ::isTombstoneOrSalesforceTagged else null,
        modifier = ::reduceByWhitelist
    )
    else -> KafkaPosterApplication()
}

fun main() = application.start()
