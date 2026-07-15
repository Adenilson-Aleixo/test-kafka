package com.kafka.rest.service

import com.kafka.rest.kafka.producer.KafkaProducer
import org.springframework.stereotype.Service

@Service
class RequestOrderService(
    private val kafkaProducer: KafkaProducer
) {
    fun <T> addEvent(topic: String, data: T) {
        kafkaProducer.addEvent(topic, data)
    }
}
