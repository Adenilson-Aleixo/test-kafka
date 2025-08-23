package com.kafka.rest.service

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class IdempotencyService(val redisTemplate: StringRedisTemplate) {
    fun tryProcess(eventId: String): Boolean {
        // Tenta gravar o ID se não existir, com expiração (TTL)
        val processed =
            redisTemplate.opsForValue()
                .setIfAbsent(
                    eventId,
                    "1",
                    Duration.ofHours(1),
                )
        return processed == true
    }
}
