package no.nav.kafka.crm

val application = KafkaPosterApplication<String, String>(envAsSettings(env_POSTER_SETTINGS))

fun main() = application.start()
