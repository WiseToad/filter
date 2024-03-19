package ru.sozvezdie.filter.config

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.*
import org.apache.kafka.streams.errors.DeserializationExceptionHandler
import org.apache.kafka.streams.kstream.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.cloud.bus.BusConstants
import org.springframework.cloud.stream.binder.kafka.support.ConsumerConfigCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.kafka.support.serializer.JsonSerde
import org.springframework.util.StringUtils
import ru.sozvezdie.filter.common.sleep
import ru.sozvezdie.filter.domain.FileInfoDownloaded
import ru.sozvezdie.filter.domain.StreamThreadInfo
import ru.sozvezdie.filter.kafkastreams.CustomKafkaStreams
import ru.sozvezdie.filter.kafkastreams.errorhandler.LogAndContinueErrorHandler
import ru.sozvezdie.filter.kafkastreams.errorhandler.LogAndFailErrorHandler
import ru.sozvezdie.filter.meter.Meter
import ru.sozvezdie.filter.service.FileProcessor

@Configuration
class KafkaConfiguration(
    private val env: Environment,
    private val applicationProperties: ApplicationProperties,
    private val kafkaProperties: KafkaProperties,
    private val objectMapper: ObjectMapper,
    private val fileProcessorProvider: ObjectProvider<FileProcessor>
) {
    companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaConfiguration::class.java)
    }

    @Volatile
    private var lastRebalancedAt: Long? = null

    @Bean
    fun consumerConfigCustomizer(): ConsumerConfigCustomizer {
        return ConsumerConfigCustomizer { consumerProperties: MutableMap<String?, Any?>, bindingName: String, _: String? ->
            if (BusConstants.INPUT == bindingName) { // the SpringCloudBus consumer
                consumerProperties[ConsumerConfig.GROUP_ID_CONFIG] = getConsumerGroup(Constant.ConsumerGroup.BUS_POSTFIX, true)
            }
        }
    }

    @Bean
    fun kafkaStreamsMetricsBinder(meterRegistry: MeterRegistry, kafkaStreams: KafkaStreams): KafkaStreamsMetrics {
        createCustomKafkaStreamsMetrics(meterRegistry, kafkaStreams)
        val kafkaStreamsMetrics = KafkaStreamsMetrics(kafkaStreams, listOf(Tag.of("spring.id", "null")))
        kafkaStreamsMetrics.bindTo(meterRegistry)
        return kafkaStreamsMetrics
    }

    private fun createCustomKafkaStreamsMetrics(meterRegistry: MeterRegistry, kafkaStreams: KafkaStreams) {
        Meter.Gauge.KAFKA_STREAM_TASK_COUNT.register(meterRegistry, arrayOf("active")) {
            kafkaStreams.metadataForLocalThreads().sumOf { it.activeTasks().size }
        }
        Meter.Gauge.KAFKA_STREAM_TASK_COUNT.register(meterRegistry, arrayOf("standby")) {
            kafkaStreams.metadataForLocalThreads().sumOf { it.standbyTasks().size }
        }
    }

    @Bean
    fun kafkaStreams(): CustomKafkaStreams {
        val properties = mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG to getConsumerGroup(),
            StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE_V2,
            StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG to getDeserializationHandler(),
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG to CooperativeStickyAssignor::class.java.name,
            ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG to Sensor.RecordingLevel.DEBUG.name
        )
        return createStreams(properties, buildStreamsTopology()).also { startKafkaStreams(it) }
    }

    private fun buildStreamsTopology(): Topology {
        val downloadedSerde = JsonSerde(object: TypeReference<FileInfoDownloaded>() {}, objectMapper).ignoreTypeHeaders().noTypeInfo()
        val builder = StreamsBuilder().apply {
            stream(getTopic("${applicationProperties.downloadedTopic}-0"), Consumed.with(Serdes.String(), downloadedSerde))
                .peek{ _, fileInfoDownloaded -> processFile(fileInfoDownloaded) }
        }
        return builder.build()
    }

    private fun processFile(fileInfoDownloaded: FileInfoDownloaded) {
        try {
            log.debug("Start processing file: ${fileInfoDownloaded.loggingTag}")
            Meter.Count.INCOMING_FILE_COUNT.increment(fileInfoDownloaded)
            fileProcessorProvider.getObject().use { fileProcessor ->
                Meter.Time.FILE_PROCESSING_TIME.record(fileInfoDownloaded) { fileProcessor.process(fileInfoDownloaded) }
            }
            log.debug("Finished processing file: ${fileInfoDownloaded.loggingTag}")
        } catch (e: Exception) {
            log.error("Error during file processing: ${fileInfoDownloaded.loggingTag}", e)
            Meter.Count.ERROR_COUNT.increment(fileInfoDownloaded)
        }
    }

    private fun startKafkaStreams(kafkaStreams: CustomKafkaStreams) {
        Thread.ofPlatform().name("scaling-", 1).start { runScaleKafkaStreams(kafkaStreams) }
        kafkaStreams.setStateListener(::streamStateListener)
        kafkaStreams.start()
    }

    private fun streamStateListener(newState: KafkaStreams.State, oldState: KafkaStreams.State) {
        log.info("KafkaStreams state change: {} -> {}", oldState, newState)
        if (oldState == KafkaStreams.State.REBALANCING &&  newState == KafkaStreams.State.RUNNING) {
            log.debug("Scheduling KafkaStreams thread scaling")
            lastRebalancedAt = System.nanoTime()
        }
    }

    private fun runScaleKafkaStreams(kafkaStreams: CustomKafkaStreams) {
        do {
            while (!Thread.interrupted() && sleep(1000L)) {
                try {
                    val secsAfterRebalance = lastRebalancedAt?.let { (System.nanoTime() - it) / 1_000_000_000 }
                    if (secsAfterRebalance != null && secsAfterRebalance >= applicationProperties.autoscaleLinger) {
                        if (scaleKafkaStreams(kafkaStreams)) {
                            lastRebalancedAt = null
                        }
                    }
                } catch (e: Exception) {
                    log.error("Failed to scale KafkaStreams threads", e)
                }
            }
        } while (!Thread.interrupted() && sleep(applicationProperties.autoscaleFailureTimeout * 1000L))
    }

    private fun scaleKafkaStreams(kafkaStreams: CustomKafkaStreams): Boolean {
        val consumerGroup = getConsumerGroup()
        val consumerGroupDescription = kafkaStreams.adminClient.describeConsumerGroups(setOf(consumerGroup)).all().get().getValue(consumerGroup)
        if (consumerGroupDescription.state() == ConsumerGroupState.STABLE) {
            log.debug("Starting KafkaStreams thread scaling")

            val threadMetadata = kafkaStreams.metadataForLocalThreads()
            log.debug("KafkaStreams thread info: {}", threadMetadata.map(::StreamThreadInfo)) // shorten info
            log.trace("KafkaStreams thread metadata: {}", threadMetadata) // detailed info

            // members (consumers) assigned to a consumer group of interest, grouped by clients (KafkaStreams instances)
            val membersByClient = consumerGroupDescription.members()
                .groupBy { it.clientId().substringBefore("-" + Constant.KAFKA_STREAM_THREAD_INFIX) }
                .toSortedMap()

            val clientCount = membersByClient.size
            val clientIndex = membersByClient.keys.indexOf(kafkaStreams.clientId).also {
                if (it < 0) throw AssertionError("Failed to obtain KafkaStreams client index among consumer group members")
            }
            val memberCount = membersByClient.getValue(kafkaStreams.clientId).size
            val partitionCount = consumerGroupDescription.members().sumOf { it.assignment().topicPartitions().size }
            val desiredCount = (partitionCount + clientIndex) / clientCount
            val rescaleCount = desiredCount - memberCount
            when {
                rescaleCount > 0 -> scaleKafkaStreamsUp(kafkaStreams, rescaleCount)
                rescaleCount < 0 -> scaleKafkaStreamsDown(kafkaStreams, -rescaleCount)
                else -> log.debug("No KafkaStreams thread scaling needed")
            }
            return true
        }
        return false
    }

    private fun scaleKafkaStreamsUp(kafkaStreams: KafkaStreams, count: Int) {
        log.info("Scaling KafkaStreams UP by {} threads", count)
        repeat(count) {
            val threadName = kafkaStreams.addStreamThread()
            log.debug("Added new KafkaStreams thread: {}", threadName.orElse("null"))
        }
    }

    private fun scaleKafkaStreamsDown(kafkaStreams: KafkaStreams, count: Int) {
        log.info("Scaling KafkaStreams DOWN by {} threads", count)
        repeat(count) {
            val threadName = kafkaStreams.removeStreamThread()
            log.debug("Removed KafkaStreams thread: {}", threadName.orElse("null"))
        }
    }

    fun getConsumerGroup(postfix: String? = null, isIndividual: Boolean? = null): String {
        var consumerGroup: String
        if (isIndividual == true || (isIndividual == null && applicationProperties.isIndividualConsumerGroup)) {
            consumerGroup = ApplicationProperties.hostName.lowercase()
            // add app name prefix to distinguish different apps running on single developer machine
            var appNamePrefix = env.getProperty(Constant.APPLICATION_NAME_PROPERTY)
            if (appNamePrefix != null) {
                appNamePrefix += Constant.ConsumerGroup.SEPARATOR_CHAR
                if (!StringUtils.startsWithIgnoreCase(consumerGroup, appNamePrefix)) {
                    consumerGroup = appNamePrefix + consumerGroup
                }
            }
        } else {
            consumerGroup = env.getRequiredProperty(Constant.APPLICATION_NAME_PROPERTY)
        }
        if (StringUtils.hasText(postfix)) {
            consumerGroup += Constant.ConsumerGroup.SEPARATOR_CHAR + postfix
        }
        return applicationProperties.kafkaPrefix + consumerGroup + applicationProperties.kafkaPostfix
    }

    fun getTopic(topic: String): String = applicationProperties.kafkaPrefix + topic + applicationProperties.kafkaPostfix

    fun createStreams(customProperties: Map<String, Any>, topology: Topology): CustomKafkaStreams {
        val properties = customProperties + kafkaProperties.buildStreamsProperties()
        return CustomKafkaStreams(topology, StreamsConfig(properties))
    }

    fun <K, V> createProducer(customProperties: Map<String, Any>, keySerializer: Serializer<K>, valueSerializer: Serializer<V>): KafkaProducer<K, V> {
        val properties = customProperties + kafkaProperties.buildProducerProperties()
        return KafkaProducer(properties, keySerializer, valueSerializer)
    }

    private fun getDeserializationHandler(): Class<out DeserializationExceptionHandler> {
        return if (applicationProperties.skipKafkaDeserializationError) LogAndContinueErrorHandler::class.java else LogAndFailErrorHandler::class.java
    }
}
