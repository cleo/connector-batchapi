package com.cleo.labs.connector.batchapi.processor.template.pojo;

public class JsonSFTP {

  public String type = "sftp";
  public String connection;
  public JsonSFTP.Incoming incoming;
  public JsonSFTP.Outgoing outgoing;
  public JsonSFTP.Connect connect;
  public JsonAction[] actions;

  public JsonSFTP(){
    this.incoming = new JsonSFTP.Incoming();
    this.outgoing = new JsonSFTP.Outgoing();
    this.connect = new JsonSFTP.Connect();
  }

  public class Connect {
    public String host;
    public int port;
    public String username;
    public String password;
    public Connect(){}
  }

  public class Outgoing {
    public Storage storage;

    public class Storage {
      public String outbox;
      public String sentbox;

      public Storage() {}
    }

    public Outgoing() {
      this.storage = new Storage();
    }

  }

  public class Incoming {
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
}
