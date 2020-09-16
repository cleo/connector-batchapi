package com.cleo.labs.connector.batchapi.processor.template.pojo;

import lombok.Getter;
import lombok.Setter;

public class CSVAction {
  @Getter@Setter
  public String alias;
  @Getter@Setter
  public String commands;
  @Getter@Setter
  public String schedule;
}
