package com.kafka.rest.controller;


import com.kafka.rest.kafka.data.OrderKafka;
import com.kafka.rest.service.RequestOrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class RequestOrderController {

    private final RequestOrderService requestOrderService;

    @Autowired
    public RequestOrderController(RequestOrderService requestOrderService) {
        this.requestOrderService = requestOrderService;
    }

    @PostMapping("/requestOrder")
    public ResponseEntity<String> save(@RequestBody OrderKafka orderKafka) {

        for (int i = 0; i < 10; i++) {
            requestOrderService.addEvent("local-cluster", orderKafka);
        }

        return ResponseEntity.ok("Success");
    }

}
