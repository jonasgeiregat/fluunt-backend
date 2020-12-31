package kafmin.topic

import kafmin.Failed
import kafmin.Reason
import kafmin.Result
import kafmin.Succeeded
import kafmin.kafka.KafkaClient
import kafmin.kafka.NewTopic
import org.apache.kafka.common.errors.InvalidReplicationFactorException
import org.apache.kafka.common.errors.TopicExistsException
import javax.inject.Singleton

@Singleton
class CreateTopicUseCase(val kafkaClient: KafkaClient) {

    fun createTopic(details: CreateTopicDetails): TopicCreation {
        val topic = NewTopic(details.name, details.numberOfPartitions, details.replicationFactor)
        try {
            kafkaClient.createTopic(topic)
        } catch (e: Exception) {
            return handleException(e, details)
        }
        return Succeeded(topic)
    }

    private fun handleException(e: Exception, details: CreateTopicDetails): TopicCreation = when (e.cause) {
        is TopicExistsException -> Failed(
            Reason(
                "TOPIC_ALREADY_EXISTS",
                "topic with name ${details.name} already exist",
                e.cause
            )
        )
        is InvalidReplicationFactorException -> Failed(
            Reason(
                "INVALID_REPLICATION_FACTOR",
                "invalid replication factor",
                e.cause
            )
        )
        else -> Failed(Reason("UNKNOWN", e.cause?.message ?: "unknown", e.cause))
    }
}

typealias TopicCreation = Result<Reason, NewTopic>

data class CreateTopicDetails(val name: String, val numberOfPartitions: Int, val replicationFactor: Short)
