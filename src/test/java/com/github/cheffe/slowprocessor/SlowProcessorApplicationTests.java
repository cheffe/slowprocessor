package com.github.cheffe.slowprocessor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.batch.core.ExitStatus.COMPLETED;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SlowProcessorApplicationTests {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  @Qualifier("loadCSV")
  private Job loadCSV;

  @Test
  public void loadCSV()throws Exception {
    jobLauncherTestUtils.setJob(loadCSV);
    JobParameters jobParameters = new JobParametersBuilder().toJobParameters();
    JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
    assertThat(jobExecution.getExitStatus(), is(COMPLETED));
  }

}
