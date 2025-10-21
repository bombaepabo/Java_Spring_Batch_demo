package com.example.Spring_batch_kafka.controller;

import com.example.Spring_batch_kafka.entity.Customer;
import com.example.Spring_batch_kafka.repository.CustomerRepository;
import com.example.Spring_batch_kafka.service.CustomerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/customers")
@AllArgsConstructor
@Slf4j
@Tag(name = "Customer Management", description = "Customer CRUD operations")
public class CustomerController {

    private final CustomerService customerService;

    @GetMapping
    @Operation(summary = "Get all customers", description = "Returns paginated list of customers")
    public ResponseEntity<Page<Customer>> getAllCustomers(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Page<Customer> customers = customerService.getAllCustomers(PageRequest.of(page, size));
        return ResponseEntity.ok(customers);
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get customer by ID")
    public ResponseEntity<Customer> getCustomerById(@PathVariable Long id) {
        return customerService.getCustomerById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/country/{country}")
    @Operation(summary = "Get customers by country")
    public ResponseEntity<List<Customer>> getCustomersByCountry(@PathVariable String country) {
        List<Customer> customers = customerService.getCustomersByCountry(country);
        return ResponseEntity.ok(customers);
    }

    @PostMapping
    @Operation(summary = "Create new customer")
    public ResponseEntity<Customer> createCustomer(@RequestBody Customer customer) {
        Customer created = customerService.createCustomer(customer);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update customer")
    public ResponseEntity<Customer> updateCustomer(
            @PathVariable Long id,
            @RequestBody Customer customer) {

        return customerService.updateCustomer(id, customer)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete customer")
    public ResponseEntity<Void> deleteCustomer(@PathVariable Long id) {
        customerService.deleteCustomer(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/count")
    @Operation(summary = "Get customer count")
    public ResponseEntity<Long> getCustomerCount() {
        Long count = customerService.getCustomerCount();
        return ResponseEntity.ok(count);
    }

    @GetMapping("/processed/count")
    @Operation(summary = "Get processed customer count")
    public ResponseEntity<Long> getProcessedCustomerCount() {
        Long count = customerService.getProcessedCustomerCount();
        return ResponseEntity.ok(count);
    }
}