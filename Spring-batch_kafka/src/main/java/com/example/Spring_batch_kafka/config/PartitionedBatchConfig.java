package com.example.Spring_batch_kafka.config;

import com.example.Spring_batch_kafka.entity.Customer;
import com.example.Spring_batch_kafka.partitioner.CustomerPartitioner;
import com.example.Spring_batch_kafka.processor.CustomerProcessor;
import com.example.Spring_batch_kafka.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@AllArgsConstructor
public class PartitionedBatchConfig {

    private CustomerRepository customerRepository;
    private CustomerPartitioner customerPartitioner;

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> partitionReader(
            @Value("#{stepExecutionContext['minValue']}") Integer minValue) {

        FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        reader.setName("partitionReader");
        reader.setLinesToSkip(minValue != null ? minValue + 1 : 1);
        reader.setLineMapper(lineMapper());
        return reader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public RepositoryItemWriter<Customer> repositoryWriter() {
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step workerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("workerStep", jobRepository)
                .<Customer, Customer>chunk(50, transactionManager)
                .reader(partitionReader(null))
                .processor(new CustomerProcessor())
                .writer(repositoryWriter())
                .build();
    }

    @Bean
    public Step masterStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", customerPartitioner)
                .step(workerStep(jobRepository, transactionManager))
                .gridSize(4)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("batch-partition-");
        executor.initialize();
        return executor;
    }

    @Bean
    public Job partitionedJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("partitionedImportCustomers", jobRepository)
                .start(masterStep(jobRepository, transactionManager))
                .build();
    }
}