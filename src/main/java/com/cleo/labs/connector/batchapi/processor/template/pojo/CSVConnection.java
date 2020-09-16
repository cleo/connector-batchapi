package com.cleo.labs.connector.batchapi.processor.template.pojo;

import lombok.Getter;
import lombok.Setter;

public class CSVConnection {

  @Getter@Setter
  private String type;
  @Getter@Setter
  private String alias;
  @Getter@Setter
  private String inbox;
  @Getter@Setter
  private String outbox;
  @Getter@Setter
  private String sentbox;
  @Getter@Setter
  private String receivedbox;
  @Getter@Setter
  private CSVAction[] actions;
}
