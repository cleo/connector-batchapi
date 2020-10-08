package com.cleo.labs.connector.batchapi.processor.versalex;

public class StubVersaLex implements VersaLex {

    @Override
    public void connect() {
    }

    @Override
    public void disconnect() {
    }

    @Override
    public String get(String host, String mailbox, String property) throws Exception {
        return null;
    }

    @Override
    public String get(String host, String property) throws Exception {
        return null;
    }

    @Override
    public void set(String host, String mailbox, String property, String value) throws Exception {
    }

    @Override
    public void set(String host, String property, String value) throws Exception {
    }

    @Override
    public String decrypt(String s) {
        return s;
    }

    @Override
    public boolean connected() {
        return false;
    }
}
