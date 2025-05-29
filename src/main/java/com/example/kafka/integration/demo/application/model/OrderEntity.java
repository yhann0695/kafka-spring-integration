package com.example.kafka.integration.demo.application.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class OrderEntity {
    @Id
    private String orderId;
    private String product;
    private double price;
    private long timestamp;
    private String priority;
}
