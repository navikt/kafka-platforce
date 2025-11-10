package no.nav.sf.pdl.kafka.kafka
import org.apache.kafka.clients.consumer.KafkaConsumer

class KafkaConsumerFactory : ConsumerFactory {
    override fun createConsumer(): KafkaConsumer<String, String?> = KafkaConsumer<String, String?>(propertiesPlain)
}
