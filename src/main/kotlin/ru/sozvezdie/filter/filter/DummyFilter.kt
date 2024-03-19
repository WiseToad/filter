package ru.sozvezdie.filter.filter

import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import redis.clients.jedis.Jedis
import ru.sozvezdie.filter.common.BinaryToken
import ru.sozvezdie.filter.config.ApplicationProperties
import ru.sozvezdie.filter.service.RedisService
import java.time.LocalDate
import java.util.function.Consumer

@Component
@Scope("prototype")
class DummyFilter(
    applicationProperties: ApplicationProperties,
    redisService: RedisService
): AbstractFilter(applicationProperties, redisService) {

    override fun getUniqueFields(jedis: Jedis, hashName: BinaryToken, checksumMap: Map<BinaryToken, Int>): Set<BinaryToken> = checksumMap.keys

    override fun getNonArrivedFields(jedis: Jedis, hashName: BinaryToken, startDate: LocalDate, endDate: LocalDate, resultConsumer: Consumer<Collection<BinaryToken>>) {}

    override fun commitFilter(jedis: Jedis, hashName: BinaryToken) {}
}
