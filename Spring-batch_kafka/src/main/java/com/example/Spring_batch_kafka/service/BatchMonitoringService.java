package com.example.Spring_batch_kafka.service;


import com.example.Spring_batch_kafka.dto.JobExecutionSummary;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@AllArgsConstructor
public class BatchMonitoringService {

    private JobExplorer jobExplorer;
    private JobOperator jobOperator;

    public List<JobExecutionSummary> getRecentJobExecutions(int count) {
        return jobExplorer.getJobNames().stream()
                .flatMap(jobName -> jobExplorer.getJobInstances(jobName, 0, count).stream())
                .flatMap(jobInstance -> jobExplorer.getJobExecutions(jobInstance).stream())
                .map(this::toSummary)
                .sorted((a, b) -> b.getStartTime().compareTo(a.getStartTime()))
                .limit(count)
                .collect(Collectors.toList());
    }

    public JobExecutionSummary getJobExecutionById(Long executionId) {
        JobExecution execution = jobExplorer.getJobExecution(executionId);
        return toSummary(execution);
    }

    public List<JobExecutionSummary> getJobExecutionsByName(String jobName, int count) {
        return jobExplorer.getJobInstances(jobName, 0, count).stream()
                .flatMap(jobInstance -> jobExplorer.getJobExecutions(jobInstance).stream())
                .map(this::toSummary)
                .collect(Collectors.toList());
    }

    public List<JobExecutionSummary> getRunningJobExecutions() {
        return jobExplorer.getJobNames().stream()
                .flatMap(jobName -> jobExplorer.getJobInstances(jobName, 0, 100).stream())
                .flatMap(jobInstance -> jobExplorer.getJobExecutions(jobInstance).stream())
                .filter(execution -> execution.getStatus() == BatchStatus.STARTED)
                .map(this::toSummary)
                .collect(Collectors.toList());
    }

    public boolean stopJobExecution(Long executionId) {
        try {
            jobOperator.stop(executionId);
            return true;
        } catch (Exception e) {
            log.error("Failed to stop job execution: {}", executionId, e);
            return false;
        }
    }

    public JobExecution restartJobExecution(Long executionId) throws Exception {
        jobOperator.restart(executionId);
        return jobExplorer.getJobExecution(executionId);
    }

    public Map<String, Object> getBatchStatistics() {
        Map<String, Object> stats = new HashMap<>();
        List<String> jobNames = jobExplorer.getJobNames();

        stats.put("totalJobs", jobNames.size());
        stats.put("runningJobs", getRunningJobExecutions().size());

        return stats;
    }

    public Map<String, Object> getJobStatistics(String jobName) {
        Map<String, Object> stats = new HashMap<>();
        List<JobExecution> executions = jobExplorer.getJobInstances(jobName, 0, 100).stream()
                .flatMap(jobInstance -> jobExplorer.getJobExecutions(jobInstance).stream())
                .collect(Collectors.toList());

        stats.put("totalExecutions", executions.size());
        stats.put("successfulExecutions", executions.stream()
                .filter(e -> e.getStatus() == BatchStatus.COMPLETED).count());
        stats.put("failedExecutions", executions.stream()
                .filter(e -> e.getStatus() == BatchStatus.FAILED).count());

        return stats;
    }

    private JobExecutionSummary toSummary(JobExecution execution) {
        return JobExecutionSummary.builder()
                .executionId(execution.getId())
                .jobName(execution.getJobInstance().getJobName())
                .status(execution.getStatus().name())
                .startTime(execution.getStartTime())
                .endTime(execution.getEndTime())
                .build();
    }
}