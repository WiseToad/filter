package ru.sozvezdie.filter.filter

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import ru.sozvezdie.filter.common.BinaryToken
import ru.sozvezdie.filter.common.*
import ru.sozvezdie.filter.concurrency.ChunkedProcessor
import ru.sozvezdie.filter.config.ApplicationProperties
import ru.sozvezdie.filter.domain.TargetElementType
import ru.sozvezdie.filter.domain.NonArrivedElement
import ru.sozvezdie.filter.domain.TargetElement
import ru.sozvezdie.filter.meter.tools.AccumulatingTimer
import ru.sozvezdie.filter.meter.Meter
import ru.sozvezdie.filter.meter.TimeMeter
import ru.sozvezdie.filter.service.RedisService
import java.nio.ByteBuffer
import java.time.Duration
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.function.Consumer

abstract class AbstractFilter(
    protected val applicationProperties: ApplicationProperties,
    private val redisService: RedisService
): ChunkedProcessor<TargetElement>(applicationProperties.uniqueDetectionPipeSize, applicationProperties.uniqueDetectionBatchSize), AutoCloseable {

    companion object {
        private val log: Logger = LoggerFactory.getLogger(AbstractFilter::class.java)
    }

    //TODO: discover metric of how many hops between different hash names occurred during file processing
    //TODO: add metrics of chunk count and size?

    private val processedHashNames = mutableSetOf<BinaryToken>()

    private val uniqueDetectionTimer = AccumulatingTimer()
    private val nonArrivedDetectionTimer = AccumulatingTimer()
    private val filterCommitTimer = AccumulatingTimer()

    override fun processChunk(elements: Collection<TargetElement>) = getUniqueElements(elements)

    fun getUniqueElements(elements: Collection<TargetElement>): Collection<TargetElement> {
        val groups: Map<BinaryToken, List<TargetElement>> = elements.groupBy { it.hashName }
        val result = mutableListOf<TargetElement>()
        groups.entries.forEach { (hashName, elements) ->
            val checksumMap: Map<BinaryToken, Int> = elements.associate { it.hashField to it.checksum }
            val uniqueFields: Set<BinaryToken> = redisService.execute(hashName.bytes) { jedis ->
                uniqueDetectionTimer.record { getUniqueFields(jedis, hashName, checksumMap) }
            }
            val uniqueElements: List<TargetElement> = elements.filter { uniqueFields.contains(it.hashField) }
            result.addAll(uniqueElements)
        }
        processedHashNames += groups.keys
        return result
    }

    protected abstract fun getUniqueFields(jedis: Jedis, hashName: BinaryToken, checksumMap: Map<BinaryToken, Int>): Set<BinaryToken>

    fun getNonArrivedElements(startDate: LocalDate, endDate: LocalDate, resultConsumer: Consumer<Collection<NonArrivedElement>>) {
        processedHashNames.forEach { hashName ->
            redisService.execute(hashName.bytes) { jedis ->
                nonArrivedDetectionTimer.record {
                    getNonArrivedFields(jedis, hashName, startDate, endDate) { hashFields ->
                        nonArrivedDetectionTimer.pause {
                            resultConsumer.accept(hashFields.map { resolveNonArrivedElement(hashName, it) })
                        }
                    }
                }
            }
        }
    }

    protected abstract fun getNonArrivedFields(jedis: Jedis, hashName: BinaryToken, startDate: LocalDate, endDate: LocalDate, resultConsumer: Consumer<Collection<BinaryToken>>)

    private fun resolveNonArrivedElement(hashName: BinaryToken, hashField: BinaryToken): NonArrivedElement {
        val buffer = ByteBuffer.wrap(hashName.bytes)
        val elementType = TargetElementType.ofPrefix(buffer.get())
        val clientId = buffer.getCompactedLong()
        val mapPharmacyId = buffer.getCompactedString()
        val elementId = elementType.hashFieldToId(hashField)
        buffer.checkNoDataLeft()

        return NonArrivedElement(elementType.typeName, clientId, mapPharmacyId, elementId)
    }

    protected abstract fun commitFilter(jedis: Jedis, hashName: BinaryToken)

    override fun close() {
        processedHashNames.forEach { hashName ->
            redisService.execute(hashName.bytes) { jedis ->
                filterCommitTimer.record { commitFilter(jedis, hashName) }
            }
        }

        recordTimeMeter(Meter.Time.UNIQUE_DETECTION_TIME, javaClass, Duration.of(uniqueDetectionTimer.nanoTime, ChronoUnit.NANOS))
        recordTimeMeter(Meter.Time.NON_ARRIVED_DETECTION_TIME, javaClass, Duration.of(nonArrivedDetectionTimer.nanoTime, ChronoUnit.NANOS))
        recordTimeMeter(Meter.Time.FILTER_COMMIT_TIME, javaClass, Duration.of(filterCommitTimer.nanoTime, ChronoUnit.NANOS))
    }

    private fun <T> recordTimeMeter(timeMeter: TimeMeter<T>, tagValues: T, duration: Duration) {
        log.debug("${timeMeter.name}: ${duration.toSeconds()}.${String.format("%03d", duration.toMillisPart())}")
        timeMeter.record(tagValues, duration)
    }
}
