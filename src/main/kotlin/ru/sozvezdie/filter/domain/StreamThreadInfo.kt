package ru.sozvezdie.filter.domain

import org.apache.kafka.streams.ThreadMetadata
import ru.sozvezdie.filter.config.Constant

data class StreamThreadInfo(
    val shortName: String,
    val state: String,
    val activeTasks: List<StreamTaskInfo>,
    val standbyTasks: List<StreamTaskInfo>
) {
    constructor(threadMetadata: ThreadMetadata): this(
        getShortName(threadMetadata.threadName()),
        threadMetadata.threadState(),
        threadMetadata.activeTasks().map(::StreamTaskInfo),
        threadMetadata.standbyTasks().map(::StreamTaskInfo)
    )

    companion object {
        private fun getShortName(threadName: String): String {
            val shortNamePostfix = threadName.substringAfterLast(Constant.KAFKA_STREAM_THREAD_INFIX, "")
            return if (shortNamePostfix.isNotEmpty()) Constant.KAFKA_STREAM_THREAD_INFIX + shortNamePostfix else threadName
        }
    }
}
