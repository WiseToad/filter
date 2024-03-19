package ru.sozvezdie.filter.domain

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.TaskMetadata

data class StreamTaskInfo(
    val partitions: List<String>
) {
    constructor(taskMetadata: TaskMetadata): this(
        taskMetadata.topicPartitions().map(TopicPartition::toString)
    )
}
