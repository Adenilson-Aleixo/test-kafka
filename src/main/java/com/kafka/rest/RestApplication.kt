package com.kafka.rest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka

@SpringBootApplication
@EnableKafka
class RestApplication

fun main(args: Array<String>) {
    runApplication<RestApplication>(*args)
}
