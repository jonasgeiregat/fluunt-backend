package kafmin.kafka

import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import javax.inject.Singleton
import org.apache.kafka.clients.admin.NewTopic as KNewTopic

@Singleton
class KafkaClient(
    private val kafkaConsumerPool: GenericObjectPool<KafkaConsumer<ByteArray, String>>,
    private val adminClient: AdminClient,
    private val kafkaProducer: KafkaProducer<ByteArray, ByteArray>
) {

    fun listTopics(): Map<String, MutableList<PartitionInfo>> {
        return givenConsumer { it.listTopics() }?.toMap() ?: emptyMap()
    }

    fun describeTopics(topic: String): TopicDescription {
        return adminClient.describeTopics(Collections.singletonList(topic))
            .all()
            .get(5, TimeUnit.SECONDS)[topic]!!
    }

    fun describeTopics(topics: Collection<String>): MutableMap<String, TopicDescription>? {
        return adminClient.describeTopics(topics)
            .all()
            .get(5, TimeUnit.SECONDS)
    }

    private fun <T> givenConsumer(action: (KafkaConsumer<ByteArray, String>) -> T): T {
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


    fun listRecords(partitions: List<PartitionListRequest>): List<RetainedMessage> {
        return givenConsumer { consumer ->
            consumer.assign(partitions.map { it.topic })
            partitions.forEach {
                seek(consumer, it.topic, it.offset)
            }
            consumer.poll(Duration.ofSeconds(5)).map {
                var key: String? = null
                if (it.key() != null) {
                    key = String(it.key())
                }
                RetainedMessage(key, it.value(), it.partition(), it.offset())
            }
                .stream()
                .limit(totalRecordCount(partitions))
                .collect(Collectors.toList())
        }
    }

    private fun totalRecordCount(partitionListRequests: List<PartitionListRequest>) =
        partitionListRequests.stream()
            .map { it.recordCount }
            .reduce(0) { t, u -> t + u }
            .toLong()

    fun publish(topic: String, message: OutboundMessage) {
        val record = ProducerRecord(topic, message.partition, message.key.toByteArray(), message.value.toByteArray())
        this.kafkaProducer.send(record)
    }

    private fun seek(
        consumer: KafkaConsumer<ByteArray, String>,
        topicPartition: TopicPartition,
        offset: Int
    ) {
        val topicOffset = calculateOffset(consumer, topicPartition, offset)
        consumer.seek(topicPartition, topicOffset)
    }

    private fun calculateOffset(
        consumer: KafkaConsumer<ByteArray, String>,
        topicPartion: TopicPartition,
        messageCount: Int
    ): Long {
        val offsetEnd = consumer.endOffsets(listOf(topicPartion))[topicPartion] ?: 0
        return if ((offsetEnd - messageCount) < 0) 0 else (offsetEnd - messageCount)
    }
}

data class ListMessageQuery(val topicName: String)

data class PartitionListRequest(val topic: TopicPartition, val recordCount: Int, val offset: Int)
