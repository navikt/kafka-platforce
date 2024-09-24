package no.nav.sf.pdl.kafka

val devContext = env(config_DEPLOY_CLUSTER) == "dev-gcp"

val application = when (env(config_DEPLOY_APP)) {
    "sf-pdl-kafka" -> KafkaPosterApplication(
        filter = if (devContext) ::isTombstoneOrSalesforceTagged /*::cherryPickedKeys*/ else null,
        modifier = ::reduceByWhitelistAndRemoveHistoricalItems
    )
    "sf-geografisktilknytning" -> KafkaPosterApplication(
        filter = if (devContext) ::cherryPickedKeys else null
    )
    else -> throw RuntimeException("Attempted to deploy unknown app")
}

fun main() = application.start()
