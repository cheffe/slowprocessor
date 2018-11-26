package com.github.cheffe.slowprocessor;

import java.util.List;
import lombok.Data;

@Data
class OutputItem {

  private int id;
  private String title;
  private List<String> words;
  private List<String> tags;

}
