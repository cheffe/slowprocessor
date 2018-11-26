package com.github.cheffe.slowprocessor;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CSVItemProcessor implements ItemProcessor<InputItem, OutputItem> {

  @Autowired
  private TaggingService taggingService;

  @Override
  public OutputItem process(InputItem inItem) throws InterruptedException {
    log.trace("process {}", inItem);
    OutputItem outItem = new OutputItem();
    outItem.setId(inItem.getId());
    outItem.setTitle(inItem.getTitle());

    List<TaggingResponse> responses = taggingService.analyze(inItem.getText());
    TaggingResponse response = responses.get(0);
    outItem.setTags(response.getTags());
    outItem.setWords(response.getWords());
    return outItem;
  }
}
