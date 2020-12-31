package kafmin.kafka

import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.TimeUnit
import javax.inject.Singleton
import org.apache.kafka.clients.admin.NewTopic as KNewTopic

@Singleton
class KafkaClient(
    private val kafkaConsumerPool: GenericObjectPool<KafkaConsumer<ByteArray, ByteArray>>,
    private val adminClient: AdminClient,
    private val kafkaProducer: KafkaProducer<ByteArray, ByteArray>
) {

    fun listTopics(): Map<String, MutableList<PartitionInfo>> {
        return givenConsumer { it.listTopics() }?.toMap() ?: emptyMap()
    }

    private fun <T> givenConsumer(action: (KafkaConsumer<ByteArray, ByteArray>) -> T): T {
        val kafkaConsumer = kafkaConsumerPool.borrowObject(2000)
        val result = action(kafkaConsumer)
        kafkaConsumer.unsubscribe()
        kafkaConsumerPool.returnObject(kafkaConsumer)
        return result
    }

    fun createTopic(topic: NewTopic): NewTopic {
        adminClient.createTopics(
            mutableListOf(
                KNewTopic(
                    topic.name,
                    topic.numberOfPartitions,
                    topic.replicationFactor
                )
            )
        )
            .all()
            .get(10, TimeUnit.SECONDS)
        return topic
    }


    fun listMessages(partitions: List<TopicPartition>, messageCount: Int): MutableList<InboundMessage> {
        return givenConsumer { consumer ->
            consumer.assign(partitions)
            partitions.forEach {
                seek(consumer, it, messageCount)
            }
            consumer.poll(Duration.ofSeconds(5)).map {
                var key: String? = null
                if (it.key() != null) {
                    key = String(it.key())
                }
                InboundMessage(key, String(it.value()), it.partition(), it.offset())
            }.toCollection(mutableListOf())
        }
    }

    fun publish(topic: String, message: OutboundMessage) {
        val record = ProducerRecord(topic, message.key.toByteArray(), message.value.toByteArray())
        this.kafkaProducer.send(record)
    }

    private fun seek(
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        topicPartition: TopicPartition,
        messageCount: Int
    ) {
        val offset = calculateOffset(consumer, topicPartition, messageCount)
        consumer.seek(topicPartition, offset)
    }

    private fun calculateOffset(
        consumer: KafkaConsumer<ByteArray, ByteArray>,
        topicPartion: TopicPartition,
        messageCount: Int
    ): Long {
        val offsetEnd = consumer.endOffsets(listOf(topicPartion)).get(topicPartion) ?: 0
        val offset = if ((offsetEnd - messageCount) < 0) 0 else (offsetEnd - messageCount)
        return offset
    }
}

data class ListMessageQuery(val topicName: String)
