package ru.sozvezdie.filter.config

import org.springframework.boot.actuate.endpoint.SanitizableData
import org.springframework.boot.actuate.endpoint.SanitizingFunction
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ActuatorConfiguration {

    private val tokens = listOf("password", "secret")

    @Bean
    fun sanitizingFunction() =
        SanitizingFunction { data ->
            if (data.key != null && tokens.any { data.key.lowercase().contains(it) }) {
                data.withValue(SanitizableData.SANITIZED_VALUE)
            } else {
                data
            }
        }
}
