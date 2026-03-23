package com.kafka.rest.kafka.consumer


import com.kafka.rest.service.IdempotencyService
import io.mockk.every
import io.mockk.verify
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka


@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = ["local-cluster"])
class OrderKafkaConsumerTest {
    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockBean
    lateinit var idempotencyService: IdempotencyService

    @Test
    fun `deve consumir mensagem do kafka`() {
        every { idempotencyService.tryProcess("123") } returns true

        kafkaTemplate.send("local-cluster", "chave", """{"id":"123","name":"teste"}""")

        await().atMost(5, TimeUnit.SECONDS).untilAsserted {
            verify { idempotencyService.tryProcess("123") }
        }
    }
}