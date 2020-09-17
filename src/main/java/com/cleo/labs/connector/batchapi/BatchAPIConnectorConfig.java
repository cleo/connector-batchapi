package com.cleo.labs.connector.batchapi;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.cleo.connector.api.interfaces.ConnectorBase;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.cleo.connector.common.ConfigFileImport;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.Operation;
import com.google.common.base.Strings;

public class BatchAPIConnectorConfig {
    private ConnectorBase client;
    private BatchAPIConnectorSchema schema;

    public BatchAPIConnectorConfig(ConnectorBase client, BatchAPIConnectorSchema schema) {
        this.client = client;
        this.schema = schema;
    }

    public Path getWorkingDirectory() throws ConnectorPropertyException {
        return Paths.get(schema.workingDirectory.getValue(client));
    }

    public String getURL() throws ConnectorPropertyException {
        return schema.url.getValue(client);
    }

    public String getUser() throws ConnectorPropertyException {
        return schema.user.getValue(client);
    }

    public String getPassword() throws ConnectorPropertyException {
        return schema.password.getValue(client);
    }

    public boolean getGeneratePasswords() throws ConnectorPropertyException {
        return schema.generatePasswords.getValue(client);
    }

    public String getExportPassword() throws ConnectorPropertyException {
        return schema.exportPassword.getValue(client);
    }

    public Operation getDefaultOperation() throws ConnectorPropertyException {
        String operation = schema.defaultOperation.getValue(client).trim();
        if (!Strings.isNullOrEmpty(operation)) {
            return Operation.valueOf(operation);
        }
        return null;
    }

    public String getTemplate() throws ConnectorPropertyException {
        return ConfigFileImport.value(schema.template.getValue(client));
    }

    public boolean getIgnoreTLSChecks() throws ConnectorPropertyException {
        return schema.ignoreTLSChecks.getValue(client);
    }

    public boolean getEnableDebug() throws ConnectorPropertyException {
        return schema.enableDebug.getValue(client);
    }
}
