package no.nav.sf.pdl.kafka.resend

import mu.KotlinLogging
import no.nav.sf.pdl.kafka.hasVergemaalEllerFremtidsfullmakt
import org.apache.kafka.clients.consumer.ConsumerRecord

object RerunUtility {
    private val log = KotlinLogging.logger { }

    const val populateCache: Boolean = true // Currently only active if also no_post set to true

    fun addToCache(records: Iterable<ConsumerRecord<String, String?>>) {
        records.forEach { cache[it.key()] = Pair(it.offset(), hasVergemaalEllerFremtidsfullmakt(it)) }
    }

    val cache: MutableMap<String, Pair<Long, Boolean>> = mutableMapOf()

    fun filterAndReport() {
        // val result = filterCache()
        log.info { "Cache size ${cache.size}, after filter size ${cache.values.filter { it.second }.size}}" }
    }
}
