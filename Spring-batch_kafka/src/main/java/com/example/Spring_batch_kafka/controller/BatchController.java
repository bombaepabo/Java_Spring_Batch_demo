package com.example.Spring_batch_kafka.controller;

import com.example.Spring_batch_kafka.dto.BatchJobRequest;
import com.example.Spring_batch_kafka.dto.JobExecutionResponse;
import com.example.Spring_batch_kafka.dto.JobExecutionSummary;
import com.example.Spring_batch_kafka.service.BatchMonitoringService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/batch")
@AllArgsConstructor
@Slf4j
@Tag(name = "Batch Processing", description = "Batch job management and execution APIs")
public class BatchController {

    private final JobLauncher jobLauncher;
    private final Job csvToKafkaJob;
    private final Job partitionedJob;
    private final BatchMonitoringService monitoringService;

    // ===================================
    // Start Batch Jobs
    // ===================================

    @PostMapping("/start-kafka-batch")
    @Operation(summary = "Start Kafka-based batch processing",
            description = "Reads CSV file and publishes to Kafka for parallel processing")
    public ResponseEntity<JobExecutionResponse> startKafkaBatch(
            @RequestBody(required = false) BatchJobRequest request) {

        try {
            log.info("Starting Kafka batch job");

            JobParameters jobParameters = buildJobParameters(request);
            JobExecution execution = jobLauncher.run(csvToKafkaJob, jobParameters);

            JobExecutionResponse response = JobExecutionResponse.builder()
                    .executionId(execution.getId())
                    .jobName(execution.getJobInstance().getJobName())
                    .status(execution.getStatus().name())
                    .startTime(execution.getStartTime())
                    .message("Kafka batch job started successfully")
                    .build();

            log.info("Kafka batch job started with execution id: {}", execution.getId());
            return ResponseEntity.ok(response);

        } catch (JobExecutionAlreadyRunningException e) {
            log.warn("Job is already running");
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(JobExecutionResponse.error("Job is already running"));

        } catch (JobRestartException e) {
            log.error("Failed to restart job", e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(JobExecutionResponse.error("Failed to restart job: " + e.getMessage()));

        } catch (JobInstanceAlreadyCompleteException e) {
            log.warn("Job instance already completed");
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(JobExecutionResponse.error("Job instance already completed"));

        } catch (Exception e) {
            log.error("Error starting Kafka batch job", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(JobExecutionResponse.error("Failed to start job: " + e.getMessage()));
        }
    }

    @PostMapping("/start-partitioned-batch")
    @Operation(summary = "Start partitioned batch processing",
            description = "Processes data using multi-threaded partitioning")
    public ResponseEntity<JobExecutionResponse> startPartitionedBatch(
            @RequestBody(required = false) BatchJobRequest request) {

        try {
            log.info("Starting partitioned batch job");

            JobParameters jobParameters = buildJobParameters(request);
            JobExecution execution = jobLauncher.run(partitionedJob, jobParameters);

            JobExecutionResponse response = JobExecutionResponse.builder()
                    .executionId(execution.getId())
                    .jobName(execution.getJobInstance().getJobName())
                    .status(execution.getStatus().name())
                    .startTime(execution.getStartTime())
                    .message("Partitioned batch job started successfully")
                    .build();

            log.info("Partitioned batch job started with execution id: {}", execution.getId());
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error starting partitioned batch job", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(JobExecutionResponse.error("Failed to start job: " + e.getMessage()));
        }
    }

    // ===================================
    // Job Status & Monitoring
    // ===================================

    @GetMapping("/status/{executionId}")
    @Operation(summary = "Get job execution status",
            description = "Returns the current status of a batch job execution")
    public ResponseEntity<JobExecutionSummary> getJobStatus(
            @Parameter(description = "Job execution ID") @PathVariable Long executionId) {

        try {
            JobExecutionSummary summary = monitoringService.getJobExecutionById(executionId);
            return ResponseEntity.ok(summary);
        } catch (Exception e) {
            log.error("Error getting job status for execution id: {}", executionId, e);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
    }

    @GetMapping("/executions")
    @Operation(summary = "Get recent job executions",
            description = "Returns list of recent batch job executions")
    public ResponseEntity<List<JobExecutionSummary>> getRecentExecutions(
            @Parameter(description = "Number of executions to retrieve")
            @RequestParam(defaultValue = "10") int count) {

        try {
            List<JobExecutionSummary> executions = monitoringService.getRecentJobExecutions(count);
            return ResponseEntity.ok(executions);
        } catch (Exception e) {
            log.error("Error getting recent executions", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/executions/{jobName}")
    @Operation(summary = "Get executions by job name",
            description = "Returns executions for a specific job")
    public ResponseEntity<List<JobExecutionSummary>> getExecutionsByJobName(
            @Parameter(description = "Job name") @PathVariable String jobName,
            @Parameter(description = "Number of executions to retrieve")
            @RequestParam(defaultValue = "10") int count) {

        try {
            List<JobExecutionSummary> executions =
                    monitoringService.getJobExecutionsByName(jobName, count);
            return ResponseEntity.ok(executions);
        } catch (Exception e) {
            log.error("Error getting executions for job: {}", jobName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/running")
    @Operation(summary = "Get running jobs",
            description = "Returns list of currently running batch jobs")
    public ResponseEntity<List<JobExecutionSummary>> getRunningJobs() {
        try {
            List<JobExecutionSummary> runningJobs = monitoringService.getRunningJobExecutions();
            return ResponseEntity.ok(runningJobs);
        } catch (Exception e) {
            log.error("Error getting running jobs", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // ===================================
    // Job Control
    // ===================================

    @PostMapping("/stop/{executionId}")
    @Operation(summary = "Stop a running job",
            description = "Attempts to stop a running batch job")
    public ResponseEntity<Map<String, String>> stopJob(
            @Parameter(description = "Job execution ID") @PathVariable Long executionId) {

        try {
            boolean stopped = monitoringService.stopJobExecution(executionId);

            Map<String, String> response = new HashMap<>();
            if (stopped) {
                response.put("message", "Job stop requested");
                response.put("executionId", executionId.toString());
                return ResponseEntity.ok(response);
            } else {
                response.put("message", "Job is not running or cannot be stopped");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }
        } catch (Exception e) {
            log.error("Error stopping job execution: {}", executionId, e);
            Map<String, String> error = new HashMap<>();
            error.put("error", "Failed to stop job: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    @PostMapping("/restart/{executionId}")
    @Operation(summary = "Restart a failed job",
            description = "Restarts a failed or stopped batch job")
    public ResponseEntity<JobExecutionResponse> restartJob(
            @Parameter(description = "Job execution ID") @PathVariable Long executionId) {

        try {
            JobExecution execution = monitoringService.restartJobExecution(executionId);

            JobExecutionResponse response = JobExecutionResponse.builder()
                    .executionId(execution.getId())
                    .jobName(execution.getJobInstance().getJobName())
                    .status(execution.getStatus().name())
                    .startTime(execution.getStartTime())
                    .message("Job restarted successfully")
                    .build();

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error restarting job execution: {}", executionId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(JobExecutionResponse.error("Failed to restart job: " + e.getMessage()));
        }
    }

    // ===================================
    // Statistics & Metrics
    // ===================================

    @GetMapping("/stats")
    @Operation(summary = "Get batch statistics",
            description = "Returns overall batch processing statistics")
    public ResponseEntity<Map<String, Object>> getBatchStatistics() {
        try {
            Map<String, Object> stats = monitoringService.getBatchStatistics();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Error getting batch statistics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/stats/{jobName}")
    @Operation(summary = "Get job-specific statistics",
            description = "Returns statistics for a specific job")
    public ResponseEntity<Map<String, Object>> getJobStatistics(
            @Parameter(description = "Job name") @PathVariable String jobName) {

        try {
            Map<String, Object> stats = monitoringService.getJobStatistics(jobName);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Error getting job statistics for: {}", jobName, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // ===================================
    // Helper Methods
    // ===================================

    private JobParameters buildJobParameters(BatchJobRequest request) {
        JobParametersBuilder builder = new JobParametersBuilder();
        builder.addLong("startTime", System.currentTimeMillis());

        if (request != null) {
            if (request.getInputFile() != null) {
                builder.addString("inputFile", request.getInputFile());
            }
            if (request.getOutputFile() != null) {
                builder.addString("outputFile", request.getOutputFile());
            }
            if (request.getChunkSize() != null) {
                builder.addLong("chunkSize", request.getChunkSize().longValue());
            }
            if (request.getParameters() != null) {
                request.getParameters().forEach(builder::addString);
            }
        }

        return builder.toJobParameters();
    }
}
