package ru.sozvezdie.filter.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.config.MeterFilter
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

@Configuration
class MetricsConfiguration {

    @Bean
    fun micrometerMeterRegistryCustomizer(env: Environment): MeterRegistryCustomizer<MeterRegistry> {
        val applicationName = env.getProperty(Constant.APPLICATION_NAME_PROPERTY, "")
        return MeterRegistryCustomizer<MeterRegistry> { registry: MeterRegistry -> registry.config()
            .commonTags("application.name", applicationName, "host.name", ApplicationProperties.hostName)
            .meterFilter(MeterFilter.renameTag("", "name", "name_"))
            .meterFilter(MeterFilter.replaceTagValues("name_", { it.replace("'", "") } ))
            .meterFilter(MeterFilter.replaceTagValues("id", { it.replace("'", "") } ))
            .meterFilter(MeterFilter.replaceTagValues("uri", { it.replace("/**", "/") } ))
            // uncomment line below in order to totally disable metrics
            //.meterFilter(MeterFilter.deny())
        }
    }
}
