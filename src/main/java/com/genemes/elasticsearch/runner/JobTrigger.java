package com.genemes.elasticsearch.runner;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class JobTrigger implements CommandLineRunner {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job createFilesStep;

    @Autowired
    private Job deleteDocumentsJob;

    @Override
    public void run(String... args) throws Exception {
        JobExecution createFilesExecution = jobLauncher.run(createFilesStep, new JobParameters());

//        if (createFilesExecution.getStatus() == BatchStatus.COMPLETED) {
//            jobLauncher.run(deleteDocumentsJob, new JobParameters());
//        }
    }
}
