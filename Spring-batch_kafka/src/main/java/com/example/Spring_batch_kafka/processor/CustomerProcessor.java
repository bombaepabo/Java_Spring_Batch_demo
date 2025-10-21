package com.example.Spring_batch_kafka.processor;


import com.example.Spring_batch_kafka.entity.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
@Slf4j
public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

    @Override
    public Customer process(Customer customer) throws Exception {
        log.debug("Processing customer: {} {}", customer.getFirstName(), customer.getLastName());

        // Business logic here
        customer.setFirstName(customer.getFirstName().toUpperCase());
        customer.setLastName(customer.getLastName().toUpperCase());
        customer.setProcessedAt(new Date());
        customer.setProcessedBy(Thread.currentThread().getName());

        // Simulate processing time
        Thread.sleep(100);

        return customer;
    }
}
