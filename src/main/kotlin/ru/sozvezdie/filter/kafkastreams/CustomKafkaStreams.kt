package ru.sozvezdie.filter.kafkastreams

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology

class CustomKafkaStreams(
    topology: Topology,
    applicationConfigs: StreamsConfig
): KafkaStreams(topology, applicationConfigs) {

    val clientId: String
        get() = super.clientId

    val adminClient: Admin
        get() = super.adminClient
}
