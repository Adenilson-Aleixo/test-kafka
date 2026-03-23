package com.kafka.rest.study_threads_coroutines_virtual

import kotlinx.coroutines.*
import java.time.Instant

fun main() = runBlocking {
    val n = 1000

    // 1️⃣ Threads tradicionais
    var start = Instant.now()
    val threads = mutableListOf<Thread>()
    repeat(n) {
        threads += Thread {
            var sum = 0L
            for (i in 0..1000) sum += i
        }.apply { start() }
    }
    threads.forEach { it.join() }
    println("Threads tradicionais: ${java.time.Duration.between(start, Instant.now()).toMillis()} ms")

    // 2️⃣ Coroutines
    start = Instant.now()
    val jobs = List(n) {
        launch(Dispatchers.Default) {
            var sum = 0L
            for (i in 0..1000) sum += i
        }
    }
    jobs.forEach { it.join() }
    println("Coroutines: ${java.time.Duration.between(start, Instant.now()).toMillis()} ms")

    // 3️⃣ Virtual Threads (Java 21)
    start = Instant.now()
    val virtualThreads = List(n) {
        Thread.ofVirtual().start {
            var sum = 0L
            for (i in 0..1000) sum += i
        }
    }
    virtualThreads.forEach { it.join() }
    println("Virtual Threads: ${java.time.Duration.between(start, Instant.now()).toMillis()} ms")
}
