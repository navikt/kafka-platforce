package no.nav.sf.pdl.kafka

val devContext = env(config_DEPLOY_CLUSTER) == "dev-gcp"

val application = when (env(config_DEPLOY_APP)) {
    "sf-pdl-kafka" -> KafkaPosterApplication(
        filter = if (devContext) ::isTombstoneOrSalesforceTagged /*::cherryPickedKeys*/ else null,
        modifier = ::reduceByWhitelist
    )
    "sf-geografisktilknytning" -> KafkaPosterApplication(
        filter = ::cherryPickedKeys
    )
    else -> KafkaPosterApplication()
}

fun main() = application.start()
