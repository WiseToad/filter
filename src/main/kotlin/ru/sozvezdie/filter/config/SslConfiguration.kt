package ru.sozvezdie.filter.config

import org.springframework.cloud.context.environment.EnvironmentChangeEvent
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.springframework.core.io.DefaultResourceLoader
import org.springframework.core.io.Resource
import java.nio.file.Paths

@Configuration
class SslConfiguration(
    private val applicationProperties: ApplicationProperties
) {
    final var trustStore: Resource = loadTrustStore()
        private set

    val trustStorePassword: String
        get() = applicationProperties.trustStorePassword

    private var systemTrustStoreEnabled = false

    private fun loadTrustStore(): Resource =
        DefaultResourceLoader().getResource(applicationProperties.trustStoreLocation)

    fun enableSystemTrustStore() {
        systemTrustStoreEnabled = true
        refreshSystemTrustStore()
    }

    private fun refreshSystemTrustStore() {
        System.setProperty(Constant.TRUST_STORE_SYSTEM_PROPERTY, Paths.get(trustStore.uri).toString())
        System.setProperty(Constant.TRUST_STORE_PASSWORD_SYSTEM_PROPERTY, applicationProperties.trustStorePassword)
    }

    @EventListener
    fun handleEnvironmentChangeEvent(ignoredEvent: EnvironmentChangeEvent?) {
        trustStore = loadTrustStore()
        if (systemTrustStoreEnabled) {
            refreshSystemTrustStore()
        }
    }
}
