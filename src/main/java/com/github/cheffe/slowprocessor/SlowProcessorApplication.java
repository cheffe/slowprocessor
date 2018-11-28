package com.github.cheffe.slowprocessor;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableBatchProcessing
@EnableAsync
@SpringBootApplication
public class SlowProcessorApplication {

  public static void main(String[] args) {
    SpringApplication.run(SlowProcessorApplication.class, args);
  }
}
