package com.kafka.rest.kafka.consumer

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kafka.rest.kafka.data.OrderKafka
import com.kafka.rest.service.IdempotencyService
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class OrderKafkaConsumer(
    private val idempotencyService: IdempotencyService
) {
    private val log = LoggerFactory.getLogger(OrderKafkaConsumer::class.java)

    // 1. Definimos o Job como Supervisor para isolar falhas
    private val consumerJob = SupervisorJob()

    // 2. Criamos o Escopo vinculado ao Job e rodando em IO
    private val scope = CoroutineScope(consumerJob + Dispatchers.IO)

    @KafkaListener(
        topics = ["\${kafka.consumer.topic.name.create-order}"],
        groupId = "RequestOrder",
        containerFactory = "kafkaBatchListenerContainerFactory",
    )
    fun run(records: List<ConsumerRecord<String, String>>) {
        for (record in records) {
            // 3. Lançamos a coroutine no nosso escopo gerenciado
            scope.launch {
                processRecord(record)
            }
        }
    }

    private fun processRecord(record: ConsumerRecord<String, String>) {
        try {
            val mapper = jacksonObjectMapper().registerModule(KotlinModule())
            val order = mapper.readValue(record.value(), OrderKafka::class.java)

            if (!idempotencyService.tryProcess(order.id)) {
                log.info("Evento {} já processado. Ignorando.", order.id)
                return
            }

            log.info("Recebido: {}", order.name)
        } catch (e: JsonProcessingException) {
            log.warn("Falha ao desserializar: {}", record.value(), e)
        } catch (e: Exception) {
            log.error("Erro inesperado no processamento da mensagem", e)
        }
    }

    @PreDestroy
    fun shutdown() {
        log.info("Encerrando OrderKafkaConsumer e cancelando coroutines...")
        // 4. Cancelamos o Job, o que propaga o cancelamento para todas as coroutines filhas
        consumerJob.cancel()
    }
}
