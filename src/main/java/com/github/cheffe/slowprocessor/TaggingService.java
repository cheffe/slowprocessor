package com.github.cheffe.slowprocessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
class TaggingService {

  private int tagNo = 1;

  List<TaggingResponse> analyze(List<InputItem> items) throws InterruptedException {
    log.debug("analyze {} items for tagging", items.size());
    Thread.sleep(6000);
    List<TaggingResponse> responses = new ArrayList<>(items.size());
    for (InputItem item : items) {
      log.trace("analyze item '{}'", item);
      TaggingResponse response = new TaggingResponse();
      response.setWords(Arrays.asList(item.getText().split(" ")));

      List<String> tags = new ArrayList<>(2);
      tags.add("tag " + tagNo);
      tagNo = 1 + ((tagNo + 1) % 4);
      response.setTags(tags);
      responses.add(response);
    }

    return responses;
  }
}
