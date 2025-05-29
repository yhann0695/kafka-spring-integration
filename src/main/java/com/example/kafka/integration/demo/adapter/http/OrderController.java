package com.example.kafka.integration.demo.adapter.http;

import com.example.kafka.integration.demo.application.message.producer.OrderProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {
    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping("/orders/batch")
    public String sendBatch(@RequestParam int count) {
        orderProducer.sendBatchOrders(count);
        return "Enviado lote de " + count + " pedidos";
    }
}
