package ru.sozvezdie.filter.filter

import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ScanParams
import ru.sozvezdie.filter.common.*
import ru.sozvezdie.filter.config.Constant
import ru.sozvezdie.filter.config.ApplicationProperties
import ru.sozvezdie.filter.service.RedisService
import ru.sozvezdie.filter.service.hscan
import java.nio.ByteBuffer
import java.time.LocalDate
import java.util.function.Consumer

@Component
@Scope("prototype")
class SessionBasedFilter(
    applicationProperties: ApplicationProperties,
    redisService: RedisService
): AbstractFilter(applicationProperties, redisService) {

    private val sessionIds = mutableMapOf<BinaryToken, Byte>()

    protected fun getOldSessionId(jedis: Jedis, hashName: BinaryToken): Byte =
        sessionIds.getOrPut(hashName) { jedis.hget(Constant.SESSION_ID, hashName.bytes).let { if (it == null || it.size != 1) 0 else it[0] } }

    protected fun getNewSessionId(oldSessionId: Byte): Byte =
        if (oldSessionId < Byte.MAX_VALUE && oldSessionId >= 0) (oldSessionId + 1).toByte() else 0

    private fun addSessionId(hashFieldBytes: ByteArray, sessionId: Byte): ByteArray = ByteBuilder().add(sessionId).add(hashFieldBytes).build()
    private fun removeSessionId(hashFieldBytes: ByteArray): ByteArray = hashFieldBytes.copyOfRange(1, hashFieldBytes.size)

    private fun extractDate(hashFieldBytes: ByteArray): LocalDate = ByteBuffer.wrap(hashFieldBytes, hashFieldBytes.size - DATE_SIZE_BYTES, DATE_SIZE_BYTES).getLocalDate()
    private fun isFieldExpired(hashFieldBytes: ByteArray, thresholdDate: LocalDate): Boolean = (extractDate(hashFieldBytes) <= thresholdDate)
    private fun isFieldInDateRange(hashFieldBytes: ByteArray, startDate: LocalDate, endDate: LocalDate) = extractDate(hashFieldBytes) in startDate..endDate

    override fun getUniqueFields(jedis: Jedis, hashName: BinaryToken, checksumMap: Map<BinaryToken, Int>): Set<BinaryToken> {
        val oldSessionId: Byte = getOldSessionId(jedis, hashName)
        val newSessionId: Byte = getNewSessionId(oldSessionId)

        val thresholdDate = LocalDate.now().minusDays(Constant.ELEMENT_EXPIRE_DAYS)

        val hashFields: Array<ByteArray> = checksumMap.keys
            .map { hashField -> addSessionId(hashField.bytes, oldSessionId) }
            .toTypedArray()

        val savedChecksums: List<Int?> = jedis.hmget(hashName.bytes, *hashFields)
            .map { it?.toInt() }

        val newChecksumMap: Map<ByteArray, ByteArray> = checksumMap
            .filter { (hashField, _) -> !isFieldExpired(hashField.bytes, thresholdDate) }
            .entries.associate { (hashField, checksum) -> addSessionId(hashField.bytes, newSessionId) to checksum.toByteArray() }

        val pipeline = jedis.pipelined()
        pipeline.hdel(hashName.bytes, *hashFields)
        pipeline.hmset(hashName.bytes, newChecksumMap)
        pipeline.expire(hashName.bytes, Constant.ELEMENT_EXPIRE_SECONDS) // for the keys dropped out of processing for a long time
        pipeline.sync()

        //TODO: test the accepted assumption that order in the hashFields/checksums is the same as in the checksumMap
        return checksumMap.entries
            .filterIndexed { i, (_, checksum) -> checksum != savedChecksums[i] }
            .map { (hashField, _) -> hashField }
            .toSet()
    }

    override fun getNonArrivedFields(jedis: Jedis, hashName: BinaryToken, startDate: LocalDate, endDate: LocalDate, resultConsumer: Consumer<Collection<BinaryToken>>) {
        val oldSessionId: Byte = getOldSessionId(jedis, hashName)
        val newSessionId: Byte = getNewSessionId(oldSessionId)

        val thresholdDate = LocalDate.now().minusDays(Constant.ELEMENT_EXPIRE_DAYS)

        val scanParams = ScanParams()
            .match(ByteBuilder().add(oldSessionId).add("*").build())
            .count(applicationProperties.nonArrivedDetectionBatchSize)
        jedis.hscan(hashName.bytes, scanParams) { scanResult: List<Map.Entry<ByteArray, ByteArray>> ->
            val (inPeriod, outOfPeriod) = scanResult
                .partition { (hashFieldBytes, _) -> isFieldInDateRange(hashFieldBytes, startDate, endDate) }

            //TODO: test if we really can interleave delete/insert operations with chunked hscan

            if (scanResult.isNotEmpty()) {
                val deletingFields: Array<ByteArray> = scanResult
                    .map { (hashFieldBytes, _) -> hashFieldBytes }
                    .toTypedArray()
                jedis.hdel(hashName.bytes, *deletingFields)
            }
            if (outOfPeriod.isNotEmpty()) {
                val newChecksumMap: Map<ByteArray, ByteArray> = outOfPeriod
                    .filter { (hashFieldBytes, _) -> !isFieldExpired(hashFieldBytes, thresholdDate) }
                    .associate { (hashFieldBytes, checksumBytes) -> hashFieldBytes.apply { set(0, newSessionId) } to checksumBytes }
                jedis.hmset(hashName.bytes, newChecksumMap)
            }
            if (inPeriod.isNotEmpty()) {
                val nonArrivedFields: List<BinaryToken> = inPeriod
                    .map { (hashFieldBytes, _) -> removeSessionId(hashFieldBytes).toBinaryToken() }
                resultConsumer.accept(nonArrivedFields)
            }
        }
    }

    override fun commitFilter(jedis: Jedis, hashName: BinaryToken) {
        val oldSessionId: Byte = getOldSessionId(jedis, hashName)
        val newSessionId: Byte = getNewSessionId(oldSessionId)

        jedis.hset(Constant.SESSION_ID, mapOf(hashName.bytes to byteArrayOf(newSessionId)))
    }
}
