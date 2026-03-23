package com.kafka.rest.kafka.consumer

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kafka.rest.kafka.data.OrderKafka
import com.kafka.rest.service.IdempotencyService
import jakarta.annotation.PreDestroy
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class OrderKafkaConsumer(
    private val idempotencyService: IdempotencyService
) {
    private val log = LoggerFactory.getLogger(OrderKafkaConsumer::class.java)
    private val pool: ExecutorService = Executors.newFixedThreadPool(THREAD_TOTAL)

    @KafkaListener(
        topics = ["\${kafka.consumer.topic.name.create-order}"],
        groupId = "RequestOrder",
        containerFactory = "kafkaBatchListenerContainerFactory",
    )
    fun run(records: List<ConsumerRecord<String, String>>) {
        for (record in records) {
            pool.submit {
                try {
                    val mapper = jacksonObjectMapper().registerModule(KotlinModule())
                    val order = mapper.readValue(record.value(), OrderKafka::class.java)

                    if (!idempotencyService.tryProcess(order.id)) {
                        println("Evento já processado. Ignorando.")
                        return@submit
                    }

                    log.info("Recebido: {}", order.name)
                } catch (e: JsonProcessingException) {
                    log.warn("Falha ao desserializar: {}", record.value(), e)
                } catch (e: Exception) {
                    log.error("Erro inesperado", e)
                }
            }
        }
    }

    @PreDestroy
    fun shutdown() {
        pool.shutdown()
        pool.awaitTermination(30, TimeUnit.SECONDS)
    }

    companion object {
        const val THREAD_TOTAL = 10
    }
}
