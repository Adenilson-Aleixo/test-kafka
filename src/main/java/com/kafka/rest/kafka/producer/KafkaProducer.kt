package com.kafka.rest.kafka.producer

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaProducer(
    private val template: KafkaTemplate<Any, Any>
) {
    fun <T> addEvent(topic: String, data: T) {
        template.send(topic, data)
    }
}
