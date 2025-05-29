package com.example.kafka.integration.demo.adapter.repository;

import com.example.kafka.integration.demo.application.model.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderEntity, String> {
}
