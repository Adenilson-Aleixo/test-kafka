package com.kafka.rest.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service

public class KafkaProducer {
    private final KafkaTemplate<Object, Object> template;

    @Autowired
    public KafkaProducer(KafkaTemplate<Object, Object> template) {
        this.template = template;
    }

    public <T> void addEvent(String topic, T data) {
        template.send(topic, data);
    }
}
