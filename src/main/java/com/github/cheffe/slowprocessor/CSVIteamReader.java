package com.github.cheffe.slowprocessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  @Value("${batch-size}")
  private int batchSize;

  @Autowired
  private FlatFileItemReader<InputItem> itemReader;

  @Autowired
  private BlockingQueue<List<InputItem>> readQueue;

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
    List<InputItem> batch = new ArrayList<>(batchSize);
    for (InputItem item = itemReader.read(); item != null; item = itemReader.read()) {
      log.trace("add item {} to batch", item);
      batch.add(item);
      contribution.incrementReadCount();

      if(batch.size() == batchSize) {
        readQueue.put(batch);
        batch = new ArrayList<>(batchSize);
      }
    }
    readQueue.put(batch);
    readQueue.put(Collections.emptyList());
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
