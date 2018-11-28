package com.github.cheffe.slowprocessor;

import lombok.Data;

@Data
public class InputItem {

  private int id;
  private String title;
  private String text;

  public static class QueueEnd extends InputItem {
  }

}
