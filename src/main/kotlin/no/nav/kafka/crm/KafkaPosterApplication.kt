package no.nav.kafka.crm

import mu.KotlinLogging

class KafkaPosterApplication<K, V>(
    /**
     * KafkaPosterApplication
     * This is the top level of the integration. Its function is to setup a server with the required
     * endpoints for the kubernetes environement (see enableNAISAPI)
     * and create a work loop that alternatives between work sessions (i.e polling from kafka until we are in sync) and
     * an interruptable pause (configured with MS_BETWEEN_WORK).
     */
    val settings: List<KafkaToSFPoster.Settings> = listOf(),
    modifier: ((String, Long) -> String)? = null,
    filter: ((String, Long) -> Boolean)? = null
) {
    val poster = KafkaToSFPoster<K, V>(settings, modifier, filter)

    private val bootstrapWaitTime = envAsLong(env_MS_BETWEEN_WORK)

    private val log = KotlinLogging.logger { }
    fun start() {
        log.info { "Starting app ${env(env_DEPLOY_APP)}  - cluster ${env(env_DEPLOY_CLUSTER)} with poster settings ${envAsSettings(env_POSTER_SETTINGS)}" }
        enableNAISAPI {
            loop()
        }
        log.info { "Finished" }
    }

    private tailrec fun loop() {
        val stop = ShutdownHook.isActive() || PrestopHook.isActive()
        when {
            stop -> Unit.also { log.info { "Stopped" } }
            !stop -> {
                poster.runWorkSession()
                conditionalWait(bootstrapWaitTime)
                loop()
            }
        }
    }
}
