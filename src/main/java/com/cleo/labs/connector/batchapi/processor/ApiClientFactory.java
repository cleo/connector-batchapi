package com.cleo.labs.connector.batchapi.processor;

public interface ApiClientFactory {

    public ApiClient getApiClient(String profile) throws Exception;

}
