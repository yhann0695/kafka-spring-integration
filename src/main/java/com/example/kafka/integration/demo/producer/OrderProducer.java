package com.example.kafka.integration.demo.producer;

import com.example.kafka.integration.demo.model.Order;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeType;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class OrderProducer {
    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);
    private final StreamBridge streamBridge;
    private final Faker faker = new Faker();
    private static final int BATCH_SIZE = 400; // Ajustado para enviar tudo de uma vez

    public OrderProducer(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @Scheduled(fixedRate = 5000)
    public void sendOrder() {
        Order order = new Order();
        order.setOrderId(UUID.randomUUID().toString());
        order.setProduct(faker.commerce().productName());
        order.setPrice(faker.number().randomDouble(2, 10, 1000));
        order.setTimestamp(System.currentTimeMillis());
        order.setPriority(faker.random().nextBoolean() ? "HIGH" : "LOW");

        String destination = "HIGH".equals(order.getPriority()) ? "urgentOrderOutput" : "orderOutput";
        logger.info("Enviando pedido individual: {} para {}", order, destination);
        boolean sent = streamBridge.send(destination, order, MimeType.valueOf("application/json"));
        if (sent) {
            logger.info("Pedido enviado com sucesso: {} para {}", order, destination);
        } else {
            logger.error("Falha ao enviar pedido: {} para {}", order, destination);
        }
        System.out.println("Enviado pedido: " + order + " para " + destination);
    }

    public CompletableFuture<Void> sendBatchOrders(int count) {
        logger.info("Iniciando envio de lote de {} pedidos", count);
        List<Order> batch = new ArrayList<>();

        // Criar todas as mensagens
        for (int i = 0; i < count; i++) {
            Order order = new Order();
            order.setOrderId(UUID.randomUUID().toString());
            order.setProduct(faker.commerce().productName());
            order.setPrice(faker.number().randomDouble(2, 10, 1000));
            order.setTimestamp(System.currentTimeMillis());
            order.setPriority("HIGH");
            batch.add(order);
        }

        // Enviar lote sequencialmente
        logger.info("Enviando lote de {} mensagens", batch.size());
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            for (int j = 0; j < batch.size(); j++) {
                Order order = batch.get(j);
                logger.debug("Enviando mensagem {}: {}", j, order);
                boolean sent = streamBridge.send("urgentOrderOutput", order, MimeType.valueOf("application/json"));
                if (!sent) {
                    logger.error("Falha ao enviar mensagem {}: {}", j, order);
                } else {
                    logger.debug("Mensagem {} enviada com sucesso", j);
                }
            }
        }).exceptionally(e -> {
            logger.error("Erro ao enviar lote: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        });

        return future.thenRun(() -> logger.info("Enviado lote de {} pedidos com sucesso", count))
                .exceptionally(e -> {
                    logger.error("Erro ao completar envio de lote: {}", e.getMessage(), e);
                    throw new RuntimeException(e);
                });
    }
}