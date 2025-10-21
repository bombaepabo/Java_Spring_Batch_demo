package com.example.Spring_batch_kafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.util.Date;

@Component
@Slf4j
public class JobCompletionListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Job {} started at {}",
                jobExecution.getJobInstance().getJobName(),
                jobExecution.getStartTime());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("Job {} completed with status: {} at {}",
                jobExecution.getJobInstance().getJobName(),
                jobExecution.getStatus(),
                jobExecution.getEndTime());

        Date startDate = Date.from(jobExecution.getStartTime().atZone(ZoneId.systemDefault()).toInstant());
        Date endDate = Date.from(jobExecution.getEndTime().atZone(ZoneId.systemDefault()).toInstant());
        long millis = endDate.getTime() - startDate.getTime();


        if (jobExecution.getEndTime() != null && jobExecution.getStartTime() != null) {
            log.info("Job execution time: {} ms",
                    millis);
        }
    }
}