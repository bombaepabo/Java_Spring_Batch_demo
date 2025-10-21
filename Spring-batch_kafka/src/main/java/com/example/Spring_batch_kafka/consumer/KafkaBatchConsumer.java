package com.example.Spring_batch_kafka.consumer;

import com.example.Spring_batch_kafka.entity.Customer;
import com.example.Spring_batch_kafka.repository.CustomerRepository;
import com.example.Spring_batch_kafka.processor.CustomerProcessor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@Slf4j
@AllArgsConstructor
public class KafkaBatchConsumer {

    private CustomerRepository customerRepository;
    private CustomerProcessor customerProcessor;

    @KafkaListener(
            topics = "${kafka.topics.customer}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${kafka.consumer.concurrency:3}"
    )
    public void consumeCustomer(Customer customer) {
        try {
            log.info("Processing customer {} on thread {}",
                    customer.getId(), Thread.currentThread().getName());

            Customer processed = customerProcessor.process(customer);
            customerRepository.save(processed);

        } catch (Exception e) {
            log.error("Error processing customer: {}", customer.getId(), e);
            throw new RuntimeException("Processing failed", e);
        }
    }

    @KafkaListener(
            topics = "${kafka.topics.customer-batch}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${kafka.consumer.concurrency:3}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeCustomerBatch(List<Customer> customers) {
        log.info("Processing batch of {} customers", customers.size());

        try {
            List<Customer> processedCustomers = customers.parallelStream()
                    .map(customer -> {
                        try {
                            return customerProcessor.process(customer);
                        } catch (Exception e) {
                            log.error("Error processing customer: {}", customer.getId(), e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            customerRepository.saveAll(processedCustomers);
            log.info("Successfully processed {} customers", processedCustomers.size());

        } catch (Exception e) {
            log.error("Error processing customer batch", e);
        }
    }
}
