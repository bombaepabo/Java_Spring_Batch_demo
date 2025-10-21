package com.example.Spring_batch_kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobExecutionResponse {
    private Long executionId;
    private String jobName;
    private String status;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String message;
    private String error;

    public static JobExecutionResponse error(String errorMessage) {
        return JobExecutionResponse.builder()
                .error(errorMessage)
                .build();
    }
}