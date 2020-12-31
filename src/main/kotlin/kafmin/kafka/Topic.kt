package kafmin.kafka

import org.apache.kafka.common.PartitionInfo
import java.util.stream.Collectors.toList

data class Topic(val name: String, private val partitionInfos: MutableList<PartitionInfo>) {

    val partitions: List<Partition> = partitionInfos.stream().map { Partition() }.collect(toList());

}

data class NewTopic(val name: String, val numberOfPartitions: Int, val replicationFactor: Short)
