package com.cleo.labs.connector.batchapi.processor.template.pojo;

import lombok.Getter;
import lombok.Setter;


public class CSVMailbox {


  @Getter@Setter
  private String Host;
  @Getter@Setter
  public String UserID;
  @Getter@Setter
  public String Password;
  @Getter@Setter
  public String SSHKeyFileName;
  @Getter@Setter
  public String LDAPUser;
  @Getter@Setter
  public String OverrideDomain;
  @Getter@Setter
  public String BaseDN;
  @Getter@Setter
  public String OverrideFilter;
  @Getter@Setter
  public String SearchFilter;
  @Getter@Setter
  public String ExtendedFilter;
  @Getter@Setter
  public String DefaultHomeDir;
  @Getter@Setter
  public String CustomHomeDir;
  @Getter@Setter
  public String WhitelistIP;
  @Getter@Setter
  public String CreateCollectName;
  @Getter@Setter
  public String CreateReceiveName;
  @Getter@Setter
  public String ActionCollect;
  @Getter@Setter
  public String ActionReceive;
  @Getter@Setter
  public String Schedule_Collect;
  @Getter@Setter
  public String Schedule_Receive;
  @Getter@Setter
  public String HostNotes;
  @Getter@Setter
  public String MailboxNotes;
  @Getter@Setter
  public String CollectActionNotes;
  @Getter@Setter
  public String ReleaseActionNotes;
  @Getter@Setter
  public String OtherFolder;
  @Getter@Setter
  public String Email;

}
