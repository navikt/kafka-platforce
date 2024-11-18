package no.nav.sf.pdl.kafka.resend

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.hasVergemaalEllerFremtidsfullmakt
import org.apache.kafka.clients.consumer.ConsumerRecord

object RerunUtility {
    private val log = KotlinLogging.logger { }

    const val populateCache: Boolean = true // Currently only active if also no_post set to true

    fun addToCache(records: Iterable<ConsumerRecord<String, String?>>) = records.forEach { cache[it.key()] = it }

    fun filterCache(): List<ConsumerRecord<String, String?>> {
        val result: MutableList<ConsumerRecord<String, String?>> = mutableListOf()
        result.addAll(cache.values.filter { hasVergemaalEllerFremtidsfullmakt(it) })
        return result
    }

    val cache: MutableMap<String, ConsumerRecord<String, String?>> = mutableMapOf()

    fun filterAndReport() {
        val result = filterCache()
        log.info { "Cache size ${cache.size}, after filter size ${result.size}" }
    }
}
