package com.example.Spring_batch_kafka.service;


import com.example.Spring_batch_kafka.entity.Customer;
import com.example.Spring_batch_kafka.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;

@Service
@AllArgsConstructor
public class CustomerService {

    private CustomerRepository customerRepository;

    public Page<Customer> getAllCustomers(Pageable pageable) {
        return customerRepository.findAll(pageable);
    }

    public Optional<Customer> getCustomerById(Long id) {
        return customerRepository.findById(id);
    }

    public List<Customer> getCustomersByCountry(String country) {
        return customerRepository.findByCountry(country);
    }

    public Customer createCustomer(Customer customer) {
        return customerRepository.save(customer);
    }

    public Optional<Customer> updateCustomer(Long id, Customer customer) {
        return customerRepository.findById(id)
                .map(existing -> {
                    existing.setFirstName(customer.getFirstName());
                    existing.setLastName(customer.getLastName());
                    existing.setEmail(customer.getEmail());
                    existing.setGender(customer.getGender());
                    existing.setContactNo(customer.getContactNo());
                    existing.setCountry(customer.getCountry());
                    existing.setDob(customer.getDob());
                    return customerRepository.save(existing);
                });
    }

    public void deleteCustomer(Long id) {
        customerRepository.deleteById(id);
    }

    public Long getCustomerCount() {
        return customerRepository.count();
    }

    public Long getProcessedCustomerCount() {
        return customerRepository.countByProcessedAtIsNotNull();
    }
}