package com.example.Spring_batch_kafka.repository;

import com.example.Spring_batch_kafka.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CustomerRepository extends JpaRepository<Customer, Long> {
    List<Customer> findByCountry(String country);
    Long countByProcessedAtIsNotNull();
}