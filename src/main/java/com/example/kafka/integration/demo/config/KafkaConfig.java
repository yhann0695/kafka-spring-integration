package com.example.kafka.integration.demo.config;

import com.example.kafka.integration.demo.model.Order;
import com.example.kafka.integration.demo.model.OrderEntity;
import com.example.kafka.integration.demo.repository.OrderRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.util.function.Consumer;

@Configuration
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.cloud.stream.bindings.orderOutput.destination:orders-topic}")
    private String topicName;

    @Value("${spring.cloud.stream.bindings.urgentOrderOutput.destination:urgent-orders-topic}")
    private String urgentTopicName;

    @Value("${spring.cloud.stream.bindings.orderOutput.producer.partition-count:6}")
    private int partitions;

    @Value("${spring.kafka.topic.replication-factor:3}")
    private short replicationFactor;

    private final OrderRepository orderRepository;
    private final MeterRegistry meterRegistry;

    public KafkaConfig(OrderRepository orderRepository, MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.meterRegistry = meterRegistry;
        logger.info("Inicializando KafkaConfig com tópicos: {} e {}", topicName, urgentTopicName);
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public NewTopic ordersTopic() {
        logger.info("Criando tópico: {} com {} partições e {} réplicas", topicName, partitions, replicationFactor);
        return new NewTopic(topicName, partitions, replicationFactor);
    }

    @Bean
    public NewTopic urgentOrdersTopic() {
        logger.info("Criando tópico: {} com {} partições e {} réplicas", urgentTopicName, partitions, replicationFactor);
        return new NewTopic(urgentTopicName, partitions, replicationFactor);
    }

    @Bean
    public NewTopic dlqTopic() {
        logger.info("Criando tópico DLQ: orders-dlq-topic com {} partições e {} réplicas", partitions, replicationFactor);
        return new NewTopic("orders-dlq-topic", partitions, replicationFactor);
    }

    @Bean
    public Consumer<Order> orderConsumer() {
        logger.info("Configurando consumidor para o tópico: {}", topicName);
        return order -> {
            logger.info("Recebendo pedido do tópico {}: {}", topicName, order);
            try {
                if (order.getPrice() <= 0) {
                    logger.error("Preço inválido no pedido: {}", order);
                    throw new IllegalArgumentException("Preço inválido: " + order.getPrice());
                }
                order.setPrice(order.getPrice() * 1.1);
                OrderEntity entity = new OrderEntity();
                entity.setOrderId(order.getOrderId());
                entity.setProduct(order.getProduct());
                entity.setPrice(order.getPrice());
                entity.setTimestamp(order.getTimestamp());
                entity.setPriority(order.getPriority());
                orderRepository.save(entity);
                logger.info("Pedido salvo no banco: {}", entity);
                meterRegistry.counter("orders.processed", "topic", topicName).increment();
                Timer.Sample sample = Timer.start(meterRegistry);
                sample.stop(meterRegistry.timer("orders.processing.time", "topic", topicName));
                logger.info("Mensagem processada e salva: {}", order);
            } catch (Exception e) {
                logger.error("Erro ao processar pedido: {}", e.getMessage(), e);
                throw e;
            }
        };
    }

    @Bean
    public Consumer<Order> urgentOrderConsumer() {
        logger.info("Configurando consumidor para o tópico: {}", urgentTopicName);
        return order -> {
            logger.info("Recebendo pedido urgente do tópico {}: {}", urgentTopicName, order);
            try {
                if (order.getPrice() <= 0) {
                    logger.error("Preço inválido no pedido urgente: {}", order);
                    throw new IllegalArgumentException("Preço inválido: " + order.getPrice());
                }
                order.setPrice(order.getPrice() * 1.1);
                OrderEntity entity = new OrderEntity();
                entity.setOrderId(order.getOrderId());
                entity.setProduct(order.getProduct());
                entity.setPrice(order.getPrice());
                entity.setTimestamp(order.getTimestamp());
                entity.setPriority(order.getPriority());
                orderRepository.save(entity);
                logger.info("Pedido urgente salvo no banco: {}", entity);
                meterRegistry.counter("orders.processed", "topic", urgentTopicName).increment();
                Timer.Sample sample = Timer.start(meterRegistry);
                sample.stop(meterRegistry.timer("orders.processing.time", "topic", urgentTopicName));
                logger.info("Mensagem processada e salva (urgente): {}", order);
            } catch (Exception e) {
                logger.error("Erro ao processar pedido urgente: {}", e.getMessage(), e);
                throw e;
            }
        };
    }
}