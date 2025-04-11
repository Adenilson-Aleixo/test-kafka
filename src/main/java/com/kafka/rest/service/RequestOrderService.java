package com.kafka.rest.service;

import com.kafka.rest.kafka.producer.KafkaProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RequestOrderService {

    private final KafkaProducer kafkaProducer;

    @Autowired
    public RequestOrderService(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public <T> void addEvent(String topic, T data) {
        kafkaProducer.addEvent(topic, data);
    }
}
