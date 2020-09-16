package com.cleo.labs.connector.batchapi.processor.template.pojo;

import lombok.Getter;
import lombok.Setter;

public class CSVSFTP extends CSVConnection{

  @Getter@Setter
  private String host;
  @Getter@Setter
  private int port;
  @Getter@Setter
  private String username;
  @Getter@Setter
  private String password;
  @Getter@Setter
  public String CreateSendName;
  @Getter@Setter
  public String CreateReceiveName;
  @Getter@Setter
  public String ActionSend;
  @Getter@Setter
  public String ActionReceive;
  @Getter@Setter
  public String Schedule_Send;
  @Getter@Setter
  public String Schedule_Receive;
}
