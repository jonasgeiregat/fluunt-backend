package kafmin.message

import kafmin.kafka.KafkaClient
import kafmin.kafka.OutboundMessage
import javax.inject.Singleton

@Singleton
class PublishMessageUseCase(
    private val kafkaClient: KafkaClient
) {
    fun publish(details: MessageDetails) {
        kafkaClient.publish(details.topic, OutboundMessage(details.key, details.value, details.partition))
    }
}

data class MessageDetails(
    val topic: String,
    val key: String,
    val value: String,
    val partition: Int?
)
