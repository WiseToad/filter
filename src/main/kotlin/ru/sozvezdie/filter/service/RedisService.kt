package ru.sozvezdie.filter.service

import org.springframework.stereotype.Service
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.params.ScanParams
import ru.sozvezdie.filter.config.Constant
import ru.sozvezdie.filter.common.ResourceLoader

@Service
class RedisService(
    private val jedisPool: JedisPool
) {
    init {
        //FIXME: move library source upgrade to migration
        registerRedisLibrary(Constant.REDIS_LIB_FILTER_FILENAME)
    }

    private final fun registerRedisLibrary(filename: String) {
        val librarySource = ResourceLoader.loadResourceAsString("/redis/lib/$filename")
        jedisPool.resource.use { jedis -> jedis.functionLoadReplace(librarySource) }
    }

    fun <R> execute(shardKey: ByteArray, action: (jedis: Jedis) -> R): R {
        //TODO: leverage the shardKey parameter to select an appropriate Redis shard
        return jedisPool.resource.use(action)
    }
}

fun Jedis.hscan(key: String, scanParams: ScanParams, action: (result: List<Map.Entry<String, String>>) -> Unit) {
    var cursor = ScanParams.SCAN_POINTER_START
    do {
        val result = hscan(key, cursor, scanParams)
        if (result.result.isNotEmpty()) {
            action(result.result)
        }
        cursor = result.cursor
    } while (cursor != ScanParams.SCAN_POINTER_START)
}

fun Jedis.hscan(key: ByteArray, scanParams: ScanParams, action: (result: List<Map.Entry<ByteArray, ByteArray>>) -> Unit) {
    var cursor = ScanParams.SCAN_POINTER_START_BINARY
    do {
        val result = hscan(key, cursor, scanParams)
        if (result.result.isNotEmpty()) {
            action(result.result)
        }
        cursor = result.cursorAsBytes
    } while (!cursor.contentEquals(ScanParams.SCAN_POINTER_START_BINARY))
}

fun Jedis.sscan(key: String, scanParams: ScanParams, action: (result: List<String>) -> Unit) {
    var cursor = ScanParams.SCAN_POINTER_START
    do {
        val result = sscan(key, cursor, scanParams)
        if (result.result.isNotEmpty()) {
            action(result.result)
        }
        cursor = result.cursor
    } while (cursor != ScanParams.SCAN_POINTER_START)
}

fun Jedis.sscan(key: ByteArray, scanParams: ScanParams, action: (result: List<ByteArray>) -> Unit) {
    var cursor = ScanParams.SCAN_POINTER_START_BINARY
    do {
        val result = sscan(key, cursor, scanParams)
        if (result.result.isNotEmpty()) {
            action(result.result)
        }
        cursor = result.cursorAsBytes
    } while (!cursor.contentEquals(ScanParams.SCAN_POINTER_START_BINARY))
}

fun Jedis.fcallReturningList(name: String, keys: List<String>, args: List<String>): List<String> {
    @Suppress("UNCHECKED_CAST")
    return when (val result = fcall(name, keys, args)) {
        is List<*> -> result as List<String>
        is Map<*, *> -> if (result.isEmpty()) emptyList() else throw RuntimeException(Constant.REDIS_UNKNOWN_RESULT_ERROR)
        else -> throw RuntimeException(Constant.REDIS_UNKNOWN_RESULT_ERROR)
    }
}

fun Jedis.fcallReturningList(name: ByteArray, keys: List<ByteArray>, args: List<ByteArray>): List<ByteArray> {
    @Suppress("UNCHECKED_CAST")
    return when (val result = fcall(name, keys, args)) {
        is List<*> -> result as List<ByteArray>
        is Map<*, *> -> if (result.isEmpty()) emptyList() else throw RuntimeException(Constant.REDIS_UNKNOWN_RESULT_ERROR)
        else -> throw RuntimeException(Constant.REDIS_UNKNOWN_RESULT_ERROR)
    }
}
