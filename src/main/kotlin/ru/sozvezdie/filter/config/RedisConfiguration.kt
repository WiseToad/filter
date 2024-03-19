package ru.sozvezdie.filter.config

import org.springframework.boot.autoconfigure.data.redis.RedisProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.util.StringUtils
import redis.clients.jedis.*

@Configuration
class RedisConfiguration {

    @Bean
    fun getRedisProperties() = RedisProperties()

    @Bean
    fun getJedisPool(redisProperties: RedisProperties, sslConfiguration: SslConfiguration): JedisPool {
        val poolConfig = getPoolConfig(redisProperties.jedis.pool)
        val hostAndPort = HostAndPort(redisProperties.host, redisProperties.port)
        val clientConfig = getClientConfig(redisProperties, sslConfiguration)
        return JedisPool(poolConfig, hostAndPort, clientConfig)
    }

    fun getClientConfig(redisProperties: RedisProperties, sslConfiguration: SslConfiguration): JedisClientConfig {
        val builder = DefaultJedisClientConfig.builder()
            .user(redisProperties.username)
            .password(redisProperties.password)
            .database(redisProperties.database)
            .socketTimeoutMillis(1800000) //FIXME: set too high temporarily for mega tests
        if (StringUtils.hasText(redisProperties.clientName)) {
            builder.clientName(redisProperties.clientName)
        }
        if (redisProperties.ssl.isEnabled) {
            sslConfiguration.enableSystemTrustStore()
            builder.ssl(true)
        }
        return builder.build()
    }

    private fun getPoolConfig(poolProperties: RedisProperties.Pool): JedisPoolConfig {
        return JedisPoolConfig().apply {
            maxTotal = poolProperties.maxActive
            maxIdle = poolProperties.maxIdle
            minIdle = poolProperties.minIdle
            if (poolProperties.timeBetweenEvictionRuns != null) {
                timeBetweenEvictionRuns = poolProperties.timeBetweenEvictionRuns
            }
            if (poolProperties.maxWait != null) {
                setMaxWait(poolProperties.maxWait)
            }
            jmxEnabled = false
        }
    }
}
