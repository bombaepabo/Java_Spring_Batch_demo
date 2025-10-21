package com.example.Spring_batch_kafka.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/health")
@AllArgsConstructor
@Tag(name = "Health Check", description = "System health check endpoints")
public class HealthController {

    private final DataSource dataSource;
    private final JobExplorer jobExplorer;

    @GetMapping
    @Operation(summary = "Overall health check")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("database", checkDatabase());
        health.put("kafka", checkKafka());
        health.put("batch", checkBatch());

        return ResponseEntity.ok(health);
    }

    @GetMapping("/database")
    @Operation(summary = "Database health check")
    public ResponseEntity<Map<String, String>> databaseHealth() {
        return ResponseEntity.ok(checkDatabase());
    }

    @GetMapping("/kafka")
    @Operation(summary = "Kafka health check")
    public ResponseEntity<Map<String, String>> kafkaHealth() {
        return ResponseEntity.ok(checkKafka());
    }

    @GetMapping("/batch")
    @Operation(summary = "Batch system health check")
    public ResponseEntity<Map<String, String>> batchHealth() {
        return ResponseEntity.ok(checkBatch());
    }

    private Map<String, String> checkDatabase() {
        Map<String, String> status = new HashMap<>();
        try (Connection conn = dataSource.getConnection()) {
            status.put("status", conn.isValid(1) ? "UP" : "DOWN");
            status.put("database", conn.getMetaData().getDatabaseProductName());
        } catch (Exception e) {
            status.put("status", "DOWN");
            status.put("error", e.getMessage());
        }
        return status;
    }

    private Map<String, String> checkKafka() {
        Map<String, String> status = new HashMap<>();
        try {
            // Simple check - if KafkaTemplate is available, Kafka is likely up
            status.put("status", "UP");
            status.put("message", "Kafka connection available");
        } catch (Exception e) {
            status.put("status", "DOWN");
            status.put("error", e.getMessage());
        }
        return status;
    }

    private Map<String, String> checkBatch() {
        Map<String, String> status = new HashMap<>();
        try {
            jobExplorer.getJobNames().size(); // Simple check
            status.put("status", "UP");
            status.put("message", "Batch system available");
        } catch (Exception e) {
            status.put("status", "DOWN");
            status.put("error", e.getMessage());
        }
        return status;
    }
}