package com.github.cheffe.slowprocessor;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.batch.core.ExitStatus.COMPLETED;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
public class SlowProcessorApplicationTests {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  @Qualifier("loadCSVwithPMC")
  private Job loadCSVwithPMC;

  @Test
  public void loadCSVwithPMC()throws Exception {
    jobLauncherTestUtils.setJob(loadCSVwithPMC);
    JobParameters jobParameters = new JobParametersBuilder().toJobParameters();
    JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
    assertThat(jobExecution.getExitStatus(), is(COMPLETED));

    int write = 0;
    int read = 0;
    for(StepExecution stepExecution : jobExecution.getStepExecutions()) {
      write += stepExecution.getWriteCount();
      read += stepExecution.getReadCount();
    }
    log.info("##### statistics #####");
    log.info("#####   read: {}", read);
    log.info("#####  write: {}", write);
    log.info("#######################");

    assertThat(write, is(read));
  }
}
