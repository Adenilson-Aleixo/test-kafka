package com.kafka.rest.kafka.consumer

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.kafka.rest.kafka.data.OrderKafka
import jakarta.annotation.PreDestroy
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class OrderKafkaConsumer {

    private val log = LoggerFactory.getLogger(OrderKafkaConsumer::class.java)

    private val mapper = ObjectMapper()
    private val executor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    private val totalProcessingTimeMs = AtomicLong(0)
    private val messageCount = AtomicLong(0)
    private val firstMessageTimestamp = AtomicLong(0)
    private val lastMessageTimestamp = AtomicLong(0)

    companion object {
        private const val TOTAL_MENSAGENS_ESPERADAS = 5000000
    }

    @KafkaListener(
        topics = ["local-cluster"],
        groupId = "RequestOrder",
        containerFactory = "kafkaBatchListenerContainerFactory"
    )
    fun run(records: List<ConsumerRecord<String, String>>) {
        executor.submit {
            val batchStart = System.currentTimeMillis()

            for (record in records) {
                try {
                    val order = mapper.readValue(record.value(), OrderKafka::class.java)
                    // log.info("Recebido: {}", order)

                    // requestOrderService.addEvent("local-cluster", order)

                    messageCount.incrementAndGet()
                } catch (e: JsonProcessingException) {
                    log.warn("Falha ao desserializar: {}", record.value(), e)
                } catch (e: Exception) {
                    log.error("Erro inesperado", e)
                }
            }

            val batchEnd = System.currentTimeMillis()
            val duration = batchEnd - batchStart

            totalProcessingTimeMs.addAndGet(duration)
            firstMessageTimestamp.compareAndSet(0, batchStart)
            lastMessageTimestamp.set(batchEnd)

            val count = messageCount.get()
            if (count >= TOTAL_MENSAGENS_ESPERADAS) {
                val wallClock =
                    Duration.ofMillis(lastMessageTimestamp.get() - firstMessageTimestamp.get())

                log.info(
                    "✅ FIM | Tempo real: {}h {}m {}s | Mensagens: {}",
                    wallClock.toHours(),
                    wallClock.toMinutesPart(),
                    wallClock.toSecondsPart(),
                    count
                )
            }
        }
    }

    @PreDestroy
    fun shutdown() {
        executor.shutdown()
    }
}
