package no.nav.sf.pdl.kafka

val devContext = env(config_DEPLOY_CLUSTER) == "dev-gcp"

val application = when (env(config_DEPLOY_APP)) {
    "sf-pdl-kafka" -> KafkaPosterApplication(
        filter = if (devContext) ::cherryPickedKeys else null,
        modifier = ::reduceByWhitelist
    )
    else -> KafkaPosterApplication(
        filter = ::cherryPickedKeys
    )
}

fun main() = application.start()
