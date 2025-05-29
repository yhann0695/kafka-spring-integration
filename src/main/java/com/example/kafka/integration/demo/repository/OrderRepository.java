package com.example.kafka.integration.demo.repository;

import com.example.kafka.integration.demo.model.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderEntity, String> {
}
