package com.kafka.rest.service


import java.time.Duration
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Service

@Service
class IdempotencyService(val redisTemplate: StringRedisTemplate) {

    fun tryProcess(eventId: String): Boolean {
        // Tenta gravar o ID se não existir, com expiração (TTL)
        val processed = redisTemplate.opsForValue()
            .setIfAbsent(
                eventId,
                "1",
                Duration.ofHours(1)
            )
        return processed == true
    }
}