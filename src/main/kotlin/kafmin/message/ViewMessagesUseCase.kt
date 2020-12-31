package kafmin.message

import kafmin.kafka.KafkaClient
import kafmin.kafka.InboundMessage
import org.apache.kafka.common.TopicPartition
import javax.inject.Singleton

@Singleton
class ViewMessagesUseCase(private val kafkaClient: KafkaClient) {
    fun viewMessages(details: ViewMessagesByPartitionsDetails): MutableList<InboundMessage> {
        val partitions = details.partitions.map {
            TopicPartition(details.topic, it)
        }.toCollection(mutableListOf())
        return kafkaClient.listMessages(partitions, details.messageCount)
    }
}

data class ViewMessagesByPartitionsDetails(
    val topic: String,
    val partitions: List<Int>,
    val messageCount: Int
)
