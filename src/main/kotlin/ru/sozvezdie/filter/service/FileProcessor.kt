package ru.sozvezdie.filter.service

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Scope
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.stereotype.Component
import ru.sozvezdie.filter.common.stringArrayOf
import ru.sozvezdie.filter.config.ApplicationProperties
import ru.sozvezdie.filter.config.AwsProperties
import ru.sozvezdie.filter.config.KafkaConfiguration
import ru.sozvezdie.filter.domain.*
import ru.sozvezdie.filter.filter.AbstractFilter
import ru.sozvezdie.filter.meter.Meter
import ru.sozvezdie.filter.meter.TimeMeter
import ru.sozvezdie.filter.meter.tools.AccumulatingTimer
import ru.sozvezdie.filter.meter.tools.CountingInputStream
import ru.sozvezdie.filter.parser.PharmacyXmlParser
import ru.sozvezdie.filter.validate.ElementValidator
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.function.Consumer

private const val TEN_MINUTES = 600_000

@Component
@Scope("prototype")
class FileProcessor(
    private val applicationProperties: ApplicationProperties,
    private val awsProperties: AwsProperties,
    private val s3Service: S3Service,
    private val parser: PharmacyXmlParser,
    filterProvider: FilterProvider,
    private val filterExecutor: FilterExecutor,
    private val kafkaConfiguration: KafkaConfiguration,
    private val objectMapper: ObjectMapper,
    private val validator: ElementValidator
): AutoCloseable {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(AbstractFilter::class.java)
    }

    private var filter = filterProvider.getObject().apply { resultConsumer = Consumer(::processUniqueElements) }

    private lateinit var fileInfo: FileInfo

    private val batchMap = mutableMapOf<String, Batch>()

    private val parserResultConsumers = mapOf<Class<out Element>, Consumer<Element>>(
        Action::class.java to Consumer { processElement(it as Action, ::fillFileInfo) },
        Batch::class.java to Consumer { processElement(it as Batch) { batch -> batchMap[batch.mapBatchId] = batch } },
        Distribution::class.java to Consumer { processElement(it as Distribution, ::sendToFilter) },
        Remnant::class.java to Consumer { processElement(it as Remnant, ::sendToFilter) }
    )

    private val parseTimer = AccumulatingTimer()

    private val kafkaProducer: KafkaProducer<String, Any> by lazy {
        val mapPharmacyIds = fileInfo.mapPharmacyId.split(",").sorted().joinToString(separator = ",")
        val producerProperties = mutableMapOf<String, Any>(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "${fileInfo.clientId}-$mapPharmacyIds",
            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG to TEN_MINUTES
        )
        val messageSerializer = JsonSerializer<Any>(objectMapper).noTypeInfo()
        kafkaConfiguration.createProducer(producerProperties, StringSerializer(), messageSerializer).apply { initTransactions(); beginTransaction() }
    }

    private val topicMap = mapOf(
        Distribution::class.java to kafkaConfiguration.getTopic(applicationProperties.distributionTopic),
        Remnant::class.java to kafkaConfiguration.getTopic(applicationProperties.remnantTopic)
    )

    init {
        parser.resultConsumerResolver = parserResultConsumers::get
        validator.batches = batchMap
    }

    fun process(fileInfoDownloaded: FileInfoDownloaded) {
        fileInfo = FileInfo(fileInfoDownloaded)
        try {
            log.debug("Filter used: {}", filter.javaClass.simpleName)
            filterExecutor.execute(filter)
            try {
                CountingInputStream(s3Service.getObject(awsProperties.bucket, fileInfo.fileId.toString())).use { stream ->
                    parseTimer.record { parser.parse(stream) }
                    fileInfo.fileSizeParsed = stream.readSize
                }
            } finally {
                validator.finish()
                filter.finish()
            }
            processNonArrivedElements()
            sendFileInfo()
            recordMetrics()
            kafkaProducer.commitTransaction()
        } catch (e: Exception) {
            kafkaProducer.abortTransaction()
            throw e
        }
    }

    private fun sendToFilter(targetElement: TargetElement) {
        joinBatch(targetElement)
        filter.putToPipe(targetElement)
    }

    private fun joinBatch(targetElement: TargetElement) {
        batchMap[targetElement.batchId]?.let(targetElement::joinBatch)
    }

    private fun <T: Element> processElement(element: T, process: (T) -> Unit = {}) {
        parseTimer.pause {
            fileInfo.elementCount.merge(element.javaClass.simpleName, 1, Int::plus)
            validator.validate(element)
            process(element)
        }
    }

    private fun processUniqueElements(elements: Collection<TargetElement>) {
        elements.forEach { element ->
            val isRemnantAsDistribution = element is Remnant && applicationProperties.sendRemnantAsDistribution
            if (isRemnantAsDistribution && (fileInfo.startDate == null || fileInfo.endDate == null)) {
                return
            }
            fileInfo.uniqueCount.merge(element.javaClass.simpleName, 1, Int::plus)
            val sendingElement = if (isRemnantAsDistribution) Distribution(element as Remnant).also(::joinBatch) else element
            val topic = topicMap[sendingElement.javaClass] ?: throw RuntimeException("Failed to resolve topic to send out the ${sendingElement.javaClass.simpleName} instance")
            kafkaProducer.send(ProducerRecord(topic, "${sendingElement.clientId}-${sendingElement.mapPharmacyId}", sendingElement))
        }
    }

    private fun processNonArrivedElements() {
        val startDate = fileInfo.startDate
        val endDate = fileInfo.endDate
        if (startDate == null || endDate == null) {
            return
        }
        val topic = kafkaConfiguration.getTopic(applicationProperties.nonArrivedTopic)
        filter.getNonArrivedElements(startDate, endDate) { elements ->
            elements.forEach { element ->
                fileInfo.nonArrivedCount.merge(element.typeName, 1, Int::plus)
                kafkaProducer.send(ProducerRecord(topic, "${element.clientId}-${element.mapPharmacyId}", element))
            }
        }
    }

    private fun fillFileInfo(action: Action) {
        fileInfo.startDate = action.startDate
        fileInfo.endDate = action.endDate
        fileInfo.clientId = action.clientId
        fileInfo.mapPharmacyId = action.mapPharmacyId
    }

    private fun sendFileInfo() {
        val topic = kafkaConfiguration.getTopic(applicationProperties.filteredTopic)
        kafkaProducer.send(ProducerRecord(topic, fileInfo))
    }

    private fun recordMetrics() {
        Meter.Count.PROCESSED_FILE_COUNT.increment(fileInfo)
        Meter.Count.PROCESSED_FILE_SIZE.increment(fileInfo, fileInfo.fileSizeParsed.toDouble())

        fileInfo.elementCount.forEach { (type, count) -> Meter.Count.PROCESSED_ELEMENT_COUNT.increment(stringArrayOf(fileInfo.clientId, type), count.toDouble()) }
        fileInfo.uniqueCount.forEach { (type, count) -> Meter.Count.UNIQUE_ELEMENT_COUNT.increment(stringArrayOf(fileInfo.clientId, type), count.toDouble()) }
        fileInfo.nonArrivedCount.forEach { (type, count) -> Meter.Count.NON_ARRIVED_ELEMENT_COUNT.increment(stringArrayOf(fileInfo.clientId, type), count.toDouble()) }

        recordTimeMeter(Meter.Time.PARSE_TIME, fileInfo, Duration.of(parseTimer.nanoTime, ChronoUnit.NANOS))
    }

    //TODO: merge or remove this copy-paste from AbstractFilter
    private fun <T> recordTimeMeter(timeMeter: TimeMeter<T>, tagValues: T, duration: Duration) {
        log.debug("${timeMeter.name}: ${duration.toSeconds()}.${String.format("%03d", duration.toMillisPart())}")
        timeMeter.record(tagValues, duration)
    }

    override fun close() {
        filter.close()
    }
}
