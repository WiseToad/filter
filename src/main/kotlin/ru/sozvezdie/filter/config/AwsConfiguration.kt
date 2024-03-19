package ru.sozvezdie.filter.config

import org.springframework.cloud.context.config.annotation.RefreshScope
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.URI

@Configuration
class AwsConfiguration(
    private val awsProperties: AwsProperties
) {

    @Bean
    @RefreshScope
    fun amazonS3(): S3Client {
        val schema = awsProperties.s3.schema ?: "http"
        val endpointURI = awsProperties.s3.endpoint?.let { URI.create("$schema://$it") }
        return S3Client.builder()
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(awsProperties.credentials.accessKey, awsProperties.credentials.secretKey)))
            .region(Region.of(awsProperties.region.static))
            .endpointOverride(endpointURI)
            .build();
    }
}
