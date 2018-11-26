package com.github.cheffe.slowprocessor.configuration;

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestConfig {
  @Bean
  public JobLauncherTestUtils jobLauncherTestUtils() {
    return new JobLauncherTestUtils();
  }
}
