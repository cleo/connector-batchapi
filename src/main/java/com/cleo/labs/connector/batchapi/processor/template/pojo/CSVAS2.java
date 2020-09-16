package com.cleo.labs.connector.batchapi.processor.template.pojo;

import lombok.Getter;
import lombok.Setter;

public class CSVAS2 extends CSVConnection {

  @Getter@Setter
  private String url;
  @Getter@Setter
  private String AS2From;
  @Getter@Setter
  private String AS2To;
  @Getter@Setter
  private String Subject;
  @Getter@Setter
  private String https;
  @Getter@Setter
  private String encrypted;
  @Getter@Setter
  private String signed;
  @Getter@Setter
  private String receipt;
  @Getter@Setter
  private String receipt_sign;
  @Getter@Setter
  private String receipt_type;
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
