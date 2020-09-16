package com.cleo.labs.connector.batchapi.processor.template.pojo;

import java.util.Map;

public class JsonAS2 {

  public String type = "as2";
  public String connection;
  public String localName;
  public String partnerName;
  public JsonAS2.Accept accept;
  public JsonAS2.Incoming incoming;
  public JsonAS2.Outgoing outgoing;
  public JsonAS2.Connect connect;
  public Map<String,JsonAction> actions;

  public JsonAS2(){
    this.accept = new JsonAS2.Accept();
    this.incoming = new JsonAS2.Incoming();
    this.outgoing = new JsonAS2.Outgoing();
    this.connect = new JsonAS2.Connect();
  }

  public class Connect {
    public String url;
    public Connect(){}
  }

  public class Outgoing {
    public String subject;
    public boolean encrypt;
    public boolean sign;
    public Receipt receipt;
    public Storage storage;

    public class Receipt {
      public String type;
      public boolean sign;

      public Receipt() {}
    }

    public class Storage {
      public String outbox;
      public String sentbox;

      public Storage() {}
    }

    public Outgoing() {
      this.receipt = new Receipt();
      this.storage = new Storage();
    }

  }

  public class Incoming {
    public boolean requireEncryption;
    public boolean requireSignature;
    public boolean requireReceiptSignature;
    public Storage storage;

    public class Storage {
      public String inbox;
      public String receivedbox;

      public Storage() {}
    }

    public Incoming() {
      this.storage = new Storage();
    }

  }

  public class Accept {
    public boolean requireSecurePort;
    public Accept(){
    }
  }
}
