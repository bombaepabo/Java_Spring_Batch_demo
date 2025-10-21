package com.example.Spring_batch_kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchJobRequest {
    private String inputFile;
    private String outputFile;
    private Integer chunkSize;
    private Map<String, String> parameters;
}
