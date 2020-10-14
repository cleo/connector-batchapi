package com.cleo.labs.connector.batchapi;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.cleo.connector.api.interfaces.ConnectorBase;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.Operation;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.OutputFormat;
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

    public Profile[] getProfiles() throws ConnectorPropertyException {
        String value = schema.profiles.getValue(client);
        return ProfileTableProperty.toProfiles(value);
    }

    public boolean getGeneratePasswords() throws ConnectorPropertyException {
        return schema.generatePasswords.getValue(client);
    }

    public String getExportPassword() throws ConnectorPropertyException {
        return schema.exportPassword.getValue(client).trim();
    }

    public Operation getDefaultOperation() throws ConnectorPropertyException {
        String operation = schema.defaultOperation.getValue(client).trim();
        if (!Strings.isNullOrEmpty(operation)) {
            return Operation.valueOf(operation);
        }
        return null;
    }

    public String getTemplate() throws ConnectorPropertyException {
        return TemplateImport.value(schema.template.getValue(client));
    }

    public OutputFormat getOutputFormat() throws ConnectorPropertyException {
        String outputFormat = schema.outputFormat.getValue(client).trim();
        if (!Strings.isNullOrEmpty(outputFormat)) {
            return OutputFormat.valueOf(outputFormat);
        }
        return OutputFormat.yaml;
    }

    public String getOutputTemplate() throws ConnectorPropertyException {
        return TemplateImport.value(schema.outputTemplate.getValue(client));
    }

    public boolean getEnableDebug() throws ConnectorPropertyException {
        return schema.enableDebug.getValue(client);
    }
}
