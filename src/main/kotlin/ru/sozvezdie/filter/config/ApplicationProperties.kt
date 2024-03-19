package ru.sozvezdie.filter.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "application")
class ApplicationProperties {

    var trustStoreLocation: String = Constant.DEFAULT_TRUST_STORE_LOCATION
    var trustStorePassword: String = Constant.DEFAULT_TRUST_STORE_PASSWORD

    var kafkaPrefix = ""
    var kafkaPostfix = ""

    var isIndividualConsumerGroup = false
    var skipKafkaDeserializationError = false

    var downloadedTopic = "downloaded"
    var distributionTopic = "distribution"
    var remnantTopic = "remnant"
    var nonArrivedTopic = "nonarrived"
    var filteredTopic = "filtered"

    var autoscaleLinger = 5
    var autoscaleFailureTimeout = 15

    var filterType = "SessionBasedFilter"
    var uniqueDetectionPipeSize = 100
    var uniqueDetectionBatchSize = 10_000
    var nonArrivedDetectionBatchSize = 10_000
    var filterCommitBatchSize = 10_000

    var sendRemnantAsDistribution = false

    companion object {
        @JvmStatic
        val hostName: String =
            System.getenv("HOSTNAME") ?:
            System.getenv("COMPUTERNAME") ?:
            throw RuntimeException("Failed to obtain host name")
    }
}
