package com.github.cheffe.slowprocessor;

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
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CSVItemProcessor implements Tasklet, StepExecutionListener {

  @Autowired
  private TaggingService taggingService;

  @Autowired
  private BlockingQueue<List<InputItem>> inputItems;
  @Autowired
  private BlockingQueue<OutputItem> outputItems;

  @Value("${batch-size}")
  private int batchSize;

  @Override
  public void beforeStep(StepExecution stepExecution) {
    log.debug("open");
    log.debug("batch-size: {}", batchSize);
    stepExecution.setStatus(BatchStatus.STARTED);
  }

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
    log.debug("execute");
    for (List<InputItem> items = inputItems.take(); items.size() > 0; items = inputItems.take()) {
      handleBatch(items);
    }
    inputItems.put(Collections.emptyList());
    outputItems.put(new OutputItem.QueueEnd());
    log.debug("execute - COMPLETED");
    return RepeatStatus.FINISHED;
  }

  private void handleBatch(List<InputItem> batch) throws InterruptedException {
    if(batch.isEmpty()) {
      return;
    }
    List<TaggingResponse> responses = taggingService.analyze(batch);
    for (int i = 0; i < batch.size(); i++) {
      log.trace("processing {}", batch.get(i));
      OutputItem outItem = new OutputItem();
      outItem.setId(batch.get(i).getId());
      outItem.setTitle(batch.get(i).getTitle());

      outItem.setTags(responses.get(i).getTags());
      outItem.setWords(responses.get(i).getWords());

      outputItems.put(outItem);
    }
    batch.clear();
  }

  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    log.debug("close");
    stepExecution.setStatus(BatchStatus.COMPLETED);
    stepExecution.setExitStatus(ExitStatus.COMPLETED);
    return ExitStatus.COMPLETED;
  }


}
