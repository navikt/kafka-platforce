package no.nav.sf.pdl.kafka

val application = KafkaPosterApplication<String, String>(envAsSettings(env_POSTER_SETTINGS))

fun main() = application.start()
