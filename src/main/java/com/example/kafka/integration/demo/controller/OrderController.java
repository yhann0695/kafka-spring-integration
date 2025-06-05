package com.example.kafka.integration.demo.controller;

import com.example.kafka.integration.demo.model.OrderEntity;
import com.example.kafka.integration.demo.producer.OrderProducer;
import com.example.kafka.integration.demo.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
public class OrderController {
    private static final Logger logger = LoggerFactory.getLogger(OrderController.class);
    private final OrderProducer orderProducer;
    private final OrderRepository orderRepository;

    public OrderController(OrderProducer orderProducer, OrderRepository orderRepository) {
        this.orderProducer = orderProducer;
        this.orderRepository = orderRepository;
    }

    @PostMapping("/orders/batch")
    public CompletableFuture<String> sendBatch(@RequestParam int count) {
        logger.info("Recebida requisição para enviar lote de {} pedidos", count);
        return orderProducer.sendBatchOrders(count)
                .thenApply(v -> {
                    logger.info("Lote de {} pedidos enviado com sucesso", count);
                    return "Enviado lote de " + count + " pedidos com sucesso";
                })
                .exceptionally(e -> {
                    logger.error("Erro ao enviar lote de {} pedidos: {}", count, e.getMessage(), e);
                    return "Erro ao enviar lote de " + count + " pedidos: " + e.getMessage();
                });
    }

    @GetMapping("/test-save")
    public String testSave() {
        OrderEntity entity = new OrderEntity();
        entity.setOrderId(UUID.randomUUID().toString());
        entity.setProduct("Test Product");
        entity.setPrice(100.0);
        entity.setTimestamp(System.currentTimeMillis());
        entity.setPriority("HIGH");
        try {
            orderRepository.save(entity);
            logger.info("Teste de salvamento bem-sucedido: {}", entity);
            return "Teste de salvamento bem-sucedido: " + entity;
        } catch (Exception e) {
            logger.error("Erro no teste de salvamento: {}", e.getMessage(), e);
            return "Erro no teste de salvamento: " + e.getMessage();
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void testDatabaseConnection() {
        try {
            OrderEntity entity = new OrderEntity();
            entity.setOrderId(UUID.randomUUID().toString());
            entity.setProduct("Startup Test");
            entity.setPrice(50.0);
            entity.setTimestamp(System.currentTimeMillis());
            entity.setPriority("LOW");
            orderRepository.save(entity);
            logger.info("Conexão com o banco testada com sucesso: {}", entity);
        } catch (Exception e) {
            logger.error("Erro ao testar conexão com o banco na inicialização: {}", e.getMessage(), e);
        }
    }
}