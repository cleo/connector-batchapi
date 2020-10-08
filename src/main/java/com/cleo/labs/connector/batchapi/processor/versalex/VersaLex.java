package com.cleo.labs.connector.batchapi.processor.versalex;

public interface VersaLex {

    public void connect();

    public void disconnect();

    public String get(String host, String mailbox, String property) throws Exception;

    public String get(String host, String property) throws Exception;

    public void set(String host, String mailbox, String property, String value) throws Exception;

    public void set(String host, String property, String value) throws Exception;

    public String decrypt(String s);

    public boolean connected();

}
