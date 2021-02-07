package kafmin.message

import kafmin.kafka.KafkaClient
import kafmin.kafka.PartitionListRequest
import kafmin.kafka.RetainedMessage
import org.apache.kafka.common.TopicPartition
import javax.inject.Singleton

@Singleton
class ViewMessagesUseCase(private val kafkaClient: KafkaClient) {

    fun viewMessages(details: ViewMessagesDetails): List<RetainedMessage> {
        val listRequest: List<PartitionListRequest> = details.partition?.let { partitionNumber ->
            getSinglePartition(details, partitionNumber)
        } ?: run {
            getAllPartitions(details)
        }
        return kafkaClient.listRecords(listRequest)
    }

    private fun getAllPartitions(details: ViewMessagesDetails): List<PartitionListRequest> {
        val numberOfPartitions = kafkaClient.describeTopics(details.topic).partitions().size
        val recordsPerPartition = details.page.size / numberOfPartitions
        val rest = details.page.size % numberOfPartitions
        return IntRange(0, numberOfPartitions - 1).map { partitionNumber ->
            if (partitionNumber == numberOfPartitions - 1) {
                PartitionListRequest(
                    TopicPartition(details.topic, partitionNumber),
                    recordsPerPartition + rest,
                    details.page.number * recordsPerPartition
                )
            } else {
                PartitionListRequest(
                    TopicPartition(details.topic, partitionNumber),
                    recordsPerPartition,
                    details.page.number * recordsPerPartition
                )
            }
        }
    }

    private fun getSinglePartition(
        details: ViewMessagesDetails,
        partitionNumber: Int
    ): List<PartitionListRequest> {
        val messagesPerPartition = details.page.size
        return listOf(
            PartitionListRequest(
                TopicPartition(details.topic, partitionNumber),
                messagesPerPartition,
                details.page.number * messagesPerPartition
            )
        )
    }
}

data class ViewMessagesDetails(
    val topic: String,
    val partition: Int?,
    val page: Page
)

data class Page(val number: Int, val size: Int)
