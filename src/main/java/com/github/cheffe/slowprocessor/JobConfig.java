package com.github.cheffe.slowprocessor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowStep;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.TaskletStepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;

@Slf4j
@Configuration
public class JobConfig {

  @Autowired
  private JobBuilderFactory jobBuilder;

  @Autowired
  private StepBuilderFactory stepFactory;

  @Autowired
  @Qualifier("applicationTaskExecutor")
  private TaskExecutor taskExecutor;

  @Value("${batch-size}")
  private int batchSize;

  @Bean
  public BlockingQueue<InputItem> inputQueue() {
    return new ArrayBlockingQueue<>(batchSize, true);
  }

  @Bean
  public BlockingQueue<OutputItem> outputQueue() {
    return new ArrayBlockingQueue<>(batchSize, true);
  }
  @Bean
  public FlatFileItemReader<InputItem> inputItemReader() {
    return new FlatFileItemReaderBuilder<InputItem>()
        .name("inputItemReader")
        .resource(new ClassPathResource("input.csv"))
        .linesToSkip(1)
        .delimited()
        .delimiter(";")
        .names(new String[]{
            "id",
            "title",
            "text"})
        .fieldSetMapper(new BeanWrapperFieldSetMapper<InputItem>() {{
          setTargetType(InputItem.class);
        }})
        .build();
  }

  @Bean
  public Job loadCSVwithPMC(Step read, Flow process, Flow write) {
    return jobBuilder.get("loadCSVwithPMC")
        .incrementer(new RunIdIncrementer())
        .flow(read)
        .split(taskExecutor)
          .add(process, write)
        .end()
        .build();
  }

  @Bean
  public Step read(CSVIteamReader itemReader) {
    return stepFactory.get("read-step").tasklet(itemReader).build();
  }

  @Bean
  @Scope("prototype")
  public Flow process(CSVItemProcessor itemProcessor) {
    return new FlowBuilder<Flow>("process-flow")
        .start(stepFactory.get("process-step").tasklet(itemProcessor).build()).build();
  }

  @Bean
  public Flow write(BlockingQueue<OutputItem> outputItems) {
    return new FlowBuilder<Flow>("write-flow")
        .start(stepFactory.get("write-step")
            .tasklet((contribution, chunkContext) -> {
              for (OutputItem item = outputItems.take(); !(item instanceof OutputItem.QueueEnd); item = outputItems.take()) {
                log.trace("writing {}", item);
                contribution.incrementWriteCount(1);
              }
              outputItems.put(new OutputItem.QueueEnd());
              return RepeatStatus.FINISHED;
            }).build())
        .build();
  }
}
