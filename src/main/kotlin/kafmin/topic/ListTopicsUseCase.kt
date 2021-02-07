package kafmin.topic

import kafmin.kafka.KafkaClient
import kafmin.kafka.Topic
import org.apache.kafka.clients.admin.TopicDescription
import javax.inject.Singleton

@Singleton
class ListTopicsUseCase(private val kafkaClient: KafkaClient) {
    fun listTopics(): List<Topic> {
        val topics = this.listIncludingInternalTopics()
        val internalTopics = filterOutInternalTopics(topics.map { it.name })
        return topics.filter { !internalTopics.contains(it.name) }
    }

    fun listIncludingInternalTopics(): List<Topic> = kafkaClient.listTopics()
        .map { Topic(it.key, it.value) }

    private fun filterOutInternalTopics(topics: List<String>) =
        kafkaClient.describeTopics(topics)
            ?.filter { isInternal(it.value) }
            ?.map { it.value.name() } ?: emptyList<String>()

    private fun isInternal(topic: TopicDescription) =
        topic.isInternal || topic.name().startsWith("_") || topic.name()
            .startsWith("docker-connect-") || topic.name().startsWith("default_ksql_")
}
