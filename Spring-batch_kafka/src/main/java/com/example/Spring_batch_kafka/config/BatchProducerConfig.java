package com.example.Spring_batch_kafka.config;

import com.example.Spring_batch_kafka.entity.Customer;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@AllArgsConstructor
public class BatchProducerConfig {

    private KafkaTemplate<String, Customer> kafkaTemplate;

    @Bean
    public FlatFileItemReader<Customer> csvReader() {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
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
    public KafkaItemWriter<String, Customer> kafkaItemWriter() {
        KafkaItemWriter<String, Customer> writer = new KafkaItemWriter<>();
        writer.setKafkaTemplate(kafkaTemplate);
        writer.setItemKeyMapper(customer -> String.valueOf(customer.getId()));
        writer.setDelete(false);
        return writer;
    }

    @Bean
    public Step csvToKafkaStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("csv-to-kafka-step", jobRepository)
                .<Customer, Customer>chunk(1000, transactionManager)
                .reader(csvReader())
                .writer(kafkaItemWriter())
                .build();
    }

    @Bean
    public Job csvToKafkaJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("csvToKafkaJob", jobRepository)
                .start(csvToKafkaStep(jobRepository, transactionManager))
                .build();
    }
}