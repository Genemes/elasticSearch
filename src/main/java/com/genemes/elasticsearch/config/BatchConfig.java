package com.genemes.elasticsearch.config;

import com.genemes.elasticsearch.listener.JobCompletionNotificationListener;
import com.genemes.elasticsearch.model.CdrSessionId;
import com.genemes.elasticsearch.reader.CreateFilesItemReader;
import com.genemes.elasticsearch.reader.DeleteDocumentsItemReader;
import com.genemes.elasticsearch.writer.CreateFilesItemWriter;
import com.genemes.elasticsearch.writer.DeleteDocumentsItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired
    @Lazy
    private JobCompletionNotificationListener jobCompletionNotificationListener;

    @Bean
    public JobRepository localJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(localTransactionManager());
        factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
        factory.setTablePrefix("BATCH_");
        return factory.getObject();
    }

    @Bean
    public PlatformTransactionManager localTransactionManager() {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public Job createFilesStep(Step createFilesStep) {
        return jobBuilderFactory.get("createFilesStep")
                .start(createFilesStep)
                .build();
    }

    @Bean
    public Step createFilesStep(CreateFilesItemReader reader, CreateFilesItemWriter writer) {
        return stepBuilderFactory.get("createFilesStep")
                .<CdrSessionId, CdrSessionId>chunk(100)
                .reader(reader)
                .writer(writer)
                .build();
    }

    @Bean
    public Job deleteDocumentsJob(Step deleteDocumentsStep){
        return jobBuilderFactory.get("deleteDocumentsStep")
                .start(deleteDocumentsStep)
                .build();
    }

    @Bean
    public Step deleteDocumentsStep(DeleteDocumentsItemReader reader, DeleteDocumentsItemWriter writer){
        return stepBuilderFactory.get("deleteDocumentsStep")
                .<String, String>chunk(10)
                .reader(reader)
                .writer(writer)
                .build();
    }
}
