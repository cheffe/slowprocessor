package com.github.cheffe.slowprocessor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Slf4j
@Configuration
public class JobConfig {

  @Autowired
  private JobBuilderFactory jobBuilder;

  @Autowired
  private StepBuilderFactory stepFactory;

  @Bean
  public Job loadCSV(Step handleCSV) {
    return jobBuilder.get("loadCSV")
        .incrementer(new RunIdIncrementer())
        .start(handleCSV)
        .build();
  }

  @Bean
  public Step handleCSV(
      FlatFileItemReader<InputItem> inputItemReader,
      CSVItemProcessor itemProcessor) {
    return stepFactory.get("handleCSV")
      .<InputItem, OutputItem>chunk(2)
      .reader(inputItemReader)
      .processor(itemProcessor)
      .writer((ItemWriter<Object>) items -> log.debug("writing {} items", items.size()))
      .build();
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

}
