package no.nav.sf.pdl.kafka.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer

interface ConsumerFactory {
    fun createConsumer(): KafkaConsumer<String, String?>
}
