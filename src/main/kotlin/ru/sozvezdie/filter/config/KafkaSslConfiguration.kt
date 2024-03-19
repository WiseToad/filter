package ru.sozvezdie.filter.config

import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class KafkaSslConfiguration {

    @Bean
    fun kafkaPropertiesPostProcessor(sslConfiguration: SslConfiguration): BeanPostProcessor =
        object: BeanPostProcessor {
            override fun postProcessBeforeInitialization(bean: Any,  beanName: String): Any {
                if (bean is KafkaProperties) {
                    val ssl = bean.ssl
                    if (ssl.trustStoreLocation == null) {
                        ssl.trustStoreLocation = sslConfiguration.trustStore
                    }
                    if (ssl.trustStorePassword == null) {
                        ssl.trustStorePassword = sslConfiguration.trustStorePassword
                    }
                }
                return bean
            }
        }
}
