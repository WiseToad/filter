package ru.sozvezdie.filter.config

object Constant {

    const val APPLICATION_NAME_PROPERTY = "spring.application.name"
    const val SERVER_PORT_PROPERTY = "server.port"
    const val SERVER_CONTEXT_PATH_PROPERTY = "server.servlet.context-path"
    const val SERVER_SSL_KEY_STORE_PROPERTY = "server.ssl.key-store"

    const val DEFAULT_TRUST_STORE_LOCATION = "classpath:ssl/truststore.jks"
    const val DEFAULT_TRUST_STORE_PASSWORD = "*****"
    const val TRUST_STORE_SYSTEM_PROPERTY = "javax.net.ssl.trustStore"
    const val TRUST_STORE_PASSWORD_SYSTEM_PROPERTY = "javax.net.ssl.trustStorePassword"

    object ConsumerGroup {
        const val BUS_POSTFIX = "-bus"
        const val SEPARATOR_CHAR = "-"
    }

    const val CHUNKED_PROCESSOR_PUT_TIMEOUT = 15
    const val CHUNKED_PROCESSOR_STOP_TIMEOUT = 15

    const val PART_DELIMITER = ':'.code.toByte()

    const val DISTRIBUTION_PREFIX = 'd'.code.toByte()
    const val REMNANT_PREFIX = 'r'.code.toByte()

    // for session-based algorithm
    val SESSION_ID = "session-id".toByteArray()

    // for set-based algorithm
    const val UNIQUE_HASH_PREFIX = 'u'.code.toByte()
    const val STORED_SET_PREFIX = 's'.code.toByte()
    const val VIEWED_SET_PREFIX = 'v'.code.toByte()
    const val NON_ARRIVED_SET_PREFIX = 'n'.code.toByte()

    const val REDIS_LIB_FILTER_FILENAME = "libFilter.lua"

    const val REDIS_UNKNOWN_RESULT_ERROR = "Unknown result type from Redis function"

    const val ELEMENT_EXPIRE_DAYS = 120L
    const val ELEMENT_EXPIRE_SECONDS = ELEMENT_EXPIRE_DAYS * 24 * 60 * 60

    const val REMNANT_AS_DISTRIBUTION_DOC_TYPE = -1

     const val KAFKA_STREAM_THREAD_INFIX = "StreamThread"
}
