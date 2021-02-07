package kafmin.kafka

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.*
import javax.inject.Singleton

class KafkaConsumerPoolFactory(private val port: Int) :
        BasePooledObjectFactory<KafkaConsumer<ByteArray, ByteArray>>() {

    override fun wrap(obj: KafkaConsumer<ByteArray, ByteArray>?): PooledObject<KafkaConsumer<ByteArray, ByteArray>> {
        return DefaultPooledObject(obj);
    }

    override fun create(): KafkaConsumer<ByteArray, ByteArray> {
        val properties = Properties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, FormatRecognizingDeserializer::class.java);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafdrop-consumer");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
        return KafkaConsumer(properties);
    }
}

@Factory
internal class Factory {

    @Value("\${kafka.port:9092}")
    protected var port: Int = 0;

    @Singleton
    fun kafkaConsumerPool(): GenericObjectPool<KafkaConsumer<ByteArray, ByteArray>> {
        return GenericObjectPool(KafkaConsumerPoolFactory(port))
    }

    @Singleton
    fun adminClient(): AdminClient {
        val properties = Properties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafdrop-consumer");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
        return AdminClient.create(properties)
    }

    @Singleton
    fun kafkaProducer(): KafkaProducer<ByteArray, ByteArray> {
        val properties = Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java);
        return KafkaProducer<ByteArray, ByteArray >(properties)
    }
}
