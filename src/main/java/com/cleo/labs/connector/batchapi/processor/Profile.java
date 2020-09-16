package com.cleo.labs.connector.batchapi.processor;

public class Profile {
    private String url = null;
    private boolean insecure = false;
    private String username = null;
    private String password = null;
    private String exportPassword = null;

    public String getUrl() {
        return url;
    }
    public Profile setUrl(String url) {
        this.url = url;
        return this;
    }
    public boolean isInsecure() {
        return insecure;
    }
    public Profile setInsecure(boolean insecure) {
        this.insecure = insecure;
        return this;
    }
    public String getUsername() {
        return username;
    }
    public Profile setUsername(String username) {
        this.username = username;
        return this;
    }
    public String getPassword() {
        return password;
    }
    public Profile setPassword(String password) {
        this.password = password;
        return this;
    }
    public String getExportPassword() {
        return exportPassword;
    }
    public Profile setExportPassword(String exportPassword) {
        this.exportPassword = exportPassword;
        return this;
    }
}
