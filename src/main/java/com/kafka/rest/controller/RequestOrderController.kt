package com.kafka.rest.controller

import com.kafka.rest.kafka.data.OrderKafka
import com.kafka.rest.service.RequestOrderService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api")
class RequestOrderController(
    private val requestOrderService: RequestOrderService
) {

    @PostMapping("/requestOrder")
    fun save(@RequestBody orderKafka: OrderKafka): ResponseEntity<String> {
        for (i in 0 until 10) {
            requestOrderService.addEvent("local-cluster", orderKafka)
        }

        return ResponseEntity.ok("Success")
    }
}
