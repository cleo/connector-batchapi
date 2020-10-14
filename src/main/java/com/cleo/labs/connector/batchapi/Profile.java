package com.cleo.labs.connector.batchapi;

import com.cleo.labs.connector.batchapi.processor.ApiClient;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

public class Profile {
    private boolean enabled;
    @SerializedName("profilename")
    private String profileName;
    private String url;
    private String user;
    private String password;
    @SerializedName("ignoretlschecks")
    private boolean ignoreTLSChecks;

    public Profile() {
        this.enabled = false;
        this.profileName = null;
        this.url = null;
        this.user = null;
        this.password = null;
        this.ignoreTLSChecks = false;
    }

    public boolean enabled() {
        return enabled;
    }
    public Profile enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }
    public String getProfileName() {
        return profileName;
    }
    public Profile setProfileName(String profileName) {
        this.profileName = profileName;
        return this;
    }
    public String url() {
        return url;
    }
    public Profile url(String url) {
        this.url = url;
        return this;
    }
    public String user() {
        return user;
    }
    public Profile user(String user) {
        this.user = user;
        return this;
    }
    public String password() {
        return password;
    }
    public Profile password(String password) {
        this.password = password;
        return this;
    }
    public boolean ignoreTLSChecks() {
        return ignoreTLSChecks;
    }
    public Profile ignoreTLSChecks(boolean ignoreTLSChecks) {
        this.ignoreTLSChecks = ignoreTLSChecks;
        return this;
    }

    public ApiClient toApiClient() throws Exception {
        return new ApiClient(url, user, password, ignoreTLSChecks);
    }

    public ApiClient toApiClientOrNull() {
        try {
            return toApiClient();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!enabled) {
            sb.append("(disabled)");
        }
        if (ignoreTLSChecks) {
            sb.append("(ignoreTLSchecks)");
        }
        if (!Strings.isNullOrEmpty(user)) {
            sb.append(user).append("@");
        }
        if (!Strings.isNullOrEmpty(url)) {
            sb.append(url);
        }
        return sb.toString();
    }
}
