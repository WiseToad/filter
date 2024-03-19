package ru.sozvezdie.filter.filter

import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ScanParams
import ru.sozvezdie.filter.common.*
import ru.sozvezdie.filter.config.ApplicationProperties
import ru.sozvezdie.filter.config.Constant
import ru.sozvezdie.filter.service.RedisService
import ru.sozvezdie.filter.service.fcallReturningList
import java.time.LocalDate
import java.util.function.Consumer

@Component
@Scope("prototype")
class SessionBasedLuaFilter(
    applicationProperties: ApplicationProperties,
    redisService: RedisService
): SessionBasedFilter(applicationProperties, redisService) {

    companion object {
        private val GET_UNIQUE_FIELDS_LUA_FUNCTION = "sessionBasedFilter_getUniqueFields".toByteArray()
        private val GET_NON_ARRIVED_FIELDS_LUA_FUNCTION = "sessionBasedFilter_getNonArrivedFields".toByteArray()
    }

    override fun getUniqueFields(jedis: Jedis, hashName: BinaryToken, checksumMap: Map<BinaryToken, Int>): Set<BinaryToken> {
        val oldSessionId: Byte = getOldSessionId(jedis, hashName)
        val newSessionId: Byte = getNewSessionId(oldSessionId)

        val thresholdDate = LocalDate.now().minusDays(Constant.ELEMENT_EXPIRE_DAYS)

        val args: List<ByteArray> = listOf(oldSessionId.toByteArray(), newSessionId.toByteArray(), thresholdDate.toByteArray()) +
                checksumMap.entries
                    .map { (hashField, checksum) -> hashField.bytes to checksum.toByteArray() }
                    .toInterleavedList()
        return jedis.fcallReturningList(GET_UNIQUE_FIELDS_LUA_FUNCTION, listOf(hashName.bytes), args)
            .map(::BinaryToken)
            .toSet()
    }

    override fun getNonArrivedFields(jedis: Jedis, hashName: BinaryToken, startDate: LocalDate, endDate: LocalDate, resultConsumer: Consumer<Collection<BinaryToken>>) {
        val oldSessionId: Byte = getOldSessionId(jedis, hashName)
        val newSessionId: Byte = getNewSessionId(oldSessionId)

        val thresholdDate = LocalDate.now().minusDays(Constant.ELEMENT_EXPIRE_DAYS)

        val startCursor = BinaryToken(ScanParams.SCAN_POINTER_START_BINARY)
        var cursor = startCursor
        do {
            val args: List<ByteArray> = listOf(
                cursor.bytes, oldSessionId.toByteArray(), newSessionId.toByteArray(), startDate.toByteArray(), endDate.toByteArray(),
                thresholdDate.toByteArray(), applicationProperties.nonArrivedDetectionBatchSize.toByteArray()
            )
            val result: List<BinaryToken> = jedis.fcallReturningList(GET_NON_ARRIVED_FIELDS_LUA_FUNCTION, listOf(hashName.bytes), args)
                .map(::BinaryToken)
            if (result.size > 1) {
                resultConsumer.accept(result.slice(1..<result.size))
            }
            cursor = result[0]
        } while (cursor != startCursor)
    }
}
