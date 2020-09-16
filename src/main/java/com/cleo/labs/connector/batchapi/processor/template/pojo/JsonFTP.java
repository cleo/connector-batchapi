package com.cleo.labs.connector.batchapi.processor.template.pojo;

public class JsonFTP {

  public String type = "ftp";
  public String connection;
  public JsonFTP.Incoming incoming;
  public JsonFTP.Outgoing outgoing;
  public JsonFTP.Connect connect;
  public JsonAction[] actions;

  public JsonFTP(){
    this.incoming = new JsonFTP.Incoming();
    this.outgoing = new JsonFTP.Outgoing();
    this.connect = new JsonFTP.Connect();
  }

  public class Connect {
    public String host;
    public int port;
    public String username;
    public String password;
    public String defaultContentType;
    public DataChannel dataChannel;

    public class DataChannel {
      public String mode;
      public int lowPort;
      public int highPort;

      public DataChannel() { }
    }
    public Connect() {
      this.dataChannel = new DataChannel();
    }
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
