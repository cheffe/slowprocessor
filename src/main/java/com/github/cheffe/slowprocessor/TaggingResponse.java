package com.github.cheffe.slowprocessor;

import java.util.List;
import lombok.Data;

@Data
class TaggingResponse {

  private List<String> words;
  private List<String> tags;

}
