package com.example.kafka.integration.demo.model;

import lombok.Data;

@Data
public class Order {
    private String orderId;
    private String product;
    private double price;
    private long timestamp;
    private String priority; // "HIGH" ou "LOW"
}
