package ru.sozvezdie.filter.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "spring.cloud.aws")
class AwsProperties {

    val s3 = S3()

    val region = Region()

    val credentials = Credentials()

    var bucket = ""

    class S3 {
        var endpoint: String? = null
        var schema: String? = null
    }

    class Region {
        var static: String? = null
    }

    class Credentials {
        var accessKey: String? = null
        var secretKey: String? = null
    }
}
