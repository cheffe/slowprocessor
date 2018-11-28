package com.github.cheffe.slowprocessor;

import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CSVIteamReader implements Tasklet, StepExecutionListener {

  @Autowired
  private FlatFileItemReader<InputItem> itemReader;

  @Autowired
  private BlockingQueue<InputItem> readQueue;

  @Override
  public void beforeStep(StepExecution stepExecution) {
    log.debug("open");
    stepExecution.setStatus(BatchStatus.STARTING);
    itemReader.open(stepExecution.getExecutionContext());
    stepExecution.setStatus(BatchStatus.STARTED);
  }

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
    log.debug("execute");
    for (InputItem item = itemReader.read(); item != null; item = itemReader.read()) {
      log.trace("put item {}", item);
      readQueue.put(item);
      contribution.incrementReadCount();
    }
    readQueue.put(new InputItem.QueueEnd());
    log.debug("execute - COMPLETED");
    contribution.setExitStatus(ExitStatus.COMPLETED);
    return RepeatStatus.FINISHED;
  }



  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    log.debug("finish");
    itemReader.close();
    stepExecution.setStatus(BatchStatus.COMPLETED);
    stepExecution.setExitStatus(ExitStatus.COMPLETED);
    return ExitStatus.COMPLETED;
  }
}
