package com.example.kafka.integration.demo.producer;

import com.example.kafka.integration.demo.model.Order;
import com.github.javafaker.Faker;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class OrderProducer {
    private final MessageChannel orderInputChannel;
    private final Faker faker = new Faker();

    public OrderProducer(@Qualifier("orderInputChannel") MessageChannel orderInputChannel) {
        this.orderInputChannel = orderInputChannel;
    }

    @Scheduled(fixedRate = 5000) // Envia a cada 5 segundos
    public void sendOrder() {
        Order order = new Order();
        order.setOrderId(UUID.randomUUID().toString());
        order.setProduct(faker.commerce().productName());
        order.setPrice(faker.number().randomDouble(2, 10, 1000));
        order.setTimestamp(System.currentTimeMillis());

        orderInputChannel.send(MessageBuilder.withPayload(order)
                .setHeader("partitionKey", order.getOrderId()) // Usa orderId como chave para consistência
                .build());
        System.out.println("Enviado pedido: " + order);
    }

    public void sendBatchOrders(int count) {
        for (int i = 0; i < count; i++) {
            Order order = new Order();
            order.setOrderId(UUID.randomUUID().toString());
            order.setProduct(faker.commerce().productName());
            order.setPrice(faker.number().randomDouble(2, 10, 1000));
            order.setTimestamp(System.currentTimeMillis());
            order.setPriority("HIGH");
            orderInputChannel.send(MessageBuilder.withPayload(order)
                    .setHeader("partitionKey", order.getOrderId())
                    .build());
        }
        System.out.println("Enviado lote de " + count + " pedidos");
    }
}