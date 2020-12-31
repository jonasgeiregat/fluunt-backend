package kafmin.topic

import kafmin.kafka.KafkaClient
import kafmin.kafka.Topic
import javax.inject.Singleton

@Singleton
class ListTopicsUseCase(val kafkaClient: KafkaClient) {
    fun listAllTopics(): List<Topic> = kafkaClient.listTopics()
            .map { Topic(it.key, it.value) }
}
