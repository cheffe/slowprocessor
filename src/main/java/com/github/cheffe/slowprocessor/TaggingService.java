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

  List<TaggingResponse> analyze(String... texts) throws InterruptedException {
    log.debug("analyze {} texts for tagging", texts.length);
    Thread.sleep(6000);
    List<TaggingResponse> responses = new ArrayList<>(texts.length);
    for (String text : texts) {
      log.trace("analyze text '{}'", text);
      TaggingResponse response = new TaggingResponse();
      response.setWords(Arrays.asList(text.split(" ")));

      List<String> tags = new ArrayList<>(2);
      tags.add("tag " + tagNo);
      tagNo = 1 + ((tagNo + 1) % 4);
      response.setTags(tags);
      responses.add(response);
    }

    return responses;
  }
}
