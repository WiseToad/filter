package ru.sozvezdie.filter

import ru.sozvezdie.filter.config.Constant
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.boot.runApplication
import org.springframework.context.event.EventListener
import org.springframework.util.StringUtils
import java.net.InetAddress
import java.net.UnknownHostException

fun main(args: Array<String>) {
    runApplication<FilterApp>(*args)
}

@SpringBootApplication
class FilterApp {

    private val log = LoggerFactory.getLogger(javaClass)

    @EventListener
    fun onApplicationStartedEvent(event: ApplicationStartedEvent) {
        val env = event.applicationContext.environment
        val appName = env.getProperty(Constant.APPLICATION_NAME_PROPERTY, "")
        val protocol = if (env.getProperty(Constant.SERVER_SSL_KEY_STORE_PROPERTY) != null) "https" else "http"
        val serverPort = env.getProperty(Constant.SERVER_PORT_PROPERTY)
        val contextPath = env.getProperty(Constant.SERVER_CONTEXT_PATH_PROPERTY)?.let {
            if (StringUtils.hasText(it)) it else null
        } ?: "/"
        val profiles = if (env.activeProfiles.isEmpty()) env.defaultProfiles else env.activeProfiles
        log.info("\n" + """
            ----------------------------------------------------------
                Application '$appName' is running! Access URLs:
                Local:       $protocol://localhost:$serverPort$contextPath
                External:    $protocol://$hostAddress:$serverPort$contextPath
                Profile(s):  ${profiles.joinToString()}
            ----------------------------------------------------------
            """.trimIndent()
        )
    }

    private val hostAddress: String
        get() {
            return try {
                InetAddress.getLocalHost().hostAddress
            } catch (e: UnknownHostException) {
                log.warn("The host name could not be determined, using 'localhost' as fallback")
                "localhost"
            }
        }
}
