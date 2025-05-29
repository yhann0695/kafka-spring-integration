package com.example.kafka.integration.demo.config;

import com.example.kafka.integration.demo.repository.OrderRepository;
import com.example.kafka.integration.demo.model.Order;
import com.example.kafka.integration.demo.model.OrderEntity;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageChannel;

@Configuration
public class KafkaIntegrationConfig {

    @Value("${spring.integration.kafka.topic.name}")
    private String topicName;

    @Value("${spring.integration.kafka.topic.partitions}")
    private int partitions;

    @Value("${spring.integration.kafka.topic.replication-factor}")
    private short replicationFactor;

    private final OrderRepository orderRepository;
    private final MeterRegistry meterRegistry;

    public KafkaIntegrationConfig(OrderRepository orderRepository, MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.meterRegistry = meterRegistry;
    }

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic(topicName, partitions, replicationFactor);
    }

    @Bean
    public NewTopic urgentOrdersTopic() {
        return new NewTopic("urgent-orders-topic", partitions, replicationFactor);
    }

    @Bean
    public NewTopic dlqTopic() {
        return new NewTopic("orders-dlq-topic", partitions, replicationFactor);
    }

    @Bean
    public IntegrationFlow kafkaProducerFlow(KafkaTemplate<String, Order> kafkaTemplate) {
        return IntegrationFlow.from("orderInputChannel")
                .route(Order.class, order -> "HIGH".equals(order.getPriority()) ? "urgent-orders-topic" : topicName,
                        mapping -> mapping
                                .subFlowMapping("urgent-orders-topic", sf -> sf.handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic("urgent-orders-topic")))
                                .subFlowMapping(topicName, sf -> sf.handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic(topicName))))
                .get();
    }

    @Bean
    public IntegrationFlow urgentKafkaConsumerFlow(ConsumerFactory<String, Order> consumerFactory) {
        return IntegrationFlow.from(Kafka.messageDrivenChannelAdapter(consumerFactory, "urgent-orders-topic"))
                .filter(Order.class, order -> order.getPrice() > 0, f -> f.discardChannel("errorChannel"))
                .transform(Order.class, order -> {
                    order.setPrice(order.getPrice() * 1.1);
                    return order;
                })
                .handle(message -> {
                    Order order = (Order) message.getPayload();
                    OrderEntity entity = new OrderEntity();
                    entity.setOrderId(order.getOrderId());
                    entity.setProduct(order.getProduct());
                    entity.setPrice(order.getPrice());
                    entity.setTimestamp(order.getTimestamp());
                    entity.setPriority(order.getPriority());
                    orderRepository.save(entity);
                    meterRegistry.counter("orders.processed", "topic", "urgent-orders-topic").increment();
                    Timer.Sample sample = Timer.start(meterRegistry);
                    sample.stop(meterRegistry.timer("orders.processing.time", "topic", "urgent-orders-topic"));
                    System.out.println("Mensagem processada e salva (urgente): " + order);
                })
                .get();
    }

    @Bean
    public IntegrationFlow kafkaConsumerFlow(ConsumerFactory<String, Order> consumerFactory) {
        return IntegrationFlow.from(Kafka.messageDrivenChannelAdapter(consumerFactory, topicName))
                .filter(Order.class, order -> order.getPrice() > 0, f -> f.discardChannel("errorChannel"))
                .transform(Order.class, order -> {
                    order.setPrice(order.getPrice() * 1.1);
                    return order;
                })
                .handle(message -> {
                    Order order = (Order) message.getPayload();
                    OrderEntity entity = new OrderEntity();
                    entity.setOrderId(order.getOrderId());
                    entity.setProduct(order.getProduct());
                    entity.setPrice(order.getPrice());
                    entity.setTimestamp(order.getTimestamp());
                    entity.setPriority(order.getPriority());
                    orderRepository.save(entity);
                    meterRegistry.counter("orders.processed", "topic", topicName).increment();
                    Timer.Sample sample = Timer.start(meterRegistry);
                    sample.stop(meterRegistry.timer("orders.processing.time", "topic", topicName));
                    System.out.println("Mensagem processada e salva: " + order);
                })
                .get();
    }

    @Bean
    public IntegrationFlow errorLoggingFlow() {
        return IntegrationFlow.from("errorChannel")
                .handle(message -> System.err.println("Erro: Pedido inválido - " + message.getPayload()))
                .get();
    }

    @Bean
    public IntegrationFlow errorDlqFlow(KafkaTemplate<String, Order> kafkaTemplate) {
        return IntegrationFlow.from("errorChannel")
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic("orders-dlq-topic"))
                .get();
    }

    @Bean
    public MessageChannel orderInputChannel() {
        return new DirectChannel();
    }
}