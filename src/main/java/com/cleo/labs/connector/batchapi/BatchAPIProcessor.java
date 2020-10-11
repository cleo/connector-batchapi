package com.cleo.labs.connector.batchapi;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.cleo.connector.api.helper.Logger;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.cleo.labs.connector.batchapi.processor.ApiClient;
import com.cleo.labs.connector.batchapi.processor.ApiClientFactory;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.Operation;
import com.cleo.labs.connector.batchapi.processor.Json;
import com.google.common.base.Strings;
import com.google.common.io.CountingOutputStream;

public class BatchAPIProcessor extends FilterOutputStream {

    private BatchAPIConnectorConfig config;
    private Path path;
    private Logger logger;
    private ByteArrayOutputStream bytes;
    private CountingOutputStream output;

    public BatchAPIProcessor(BatchAPIConnectorConfig config, Path path, Map<String,String> metadata, Logger logger) {
        super(null);
        this.config = config;
        this.path = path;
        this.logger = logger;

        this.bytes = new ByteArrayOutputStream();
        this.output = new CountingOutputStream(bytes);
        out = output;
    }

    private ApiClientFactory getApiClientFactory() {
        return new ApiClientFactory() {
            @Override
            public ApiClient getApiClient(String profileName) throws Exception {
                Profile selected = null;     // set if we gert a match by name
                Profile firstEnabled = null; // the first enabled profile
                Profile firstDefault = null; // the first enabled profile named "default"
                Profile[] profiles = config.getProfiles();
                for (Profile profile : profiles) {
                    if (profile.enabled()) {
                        if (firstEnabled == null) {
                            firstEnabled = profile;
                        }
                        if (firstDefault == null && "default".equals(profile.getProfileName())) {
                            firstDefault = profile;
                        }
                        if (Strings.nullToEmpty(profileName).equals(Strings.nullToEmpty(profile.getProfileName()))) {
                            selected = profile;
                            break;
                        }
                    }
                }
                if (selected == null && Strings.isNullOrEmpty(profileName)) {
                    selected = firstDefault != null ? firstDefault : firstEnabled;
                }
                if (selected == null) {
                    throw new Exception("profile "+profileName+" not found");
                }
                return new ApiClient(selected.url(), selected.user(), selected.password(), selected.ignoreTLSChecks());
            }
        };
    }

    private void process(Path result) throws IOException {
        ApiClientFactory factory = null;
        try {
            if (config.getDefaultOperation() != Operation.preview) {
                factory = getApiClientFactory();
            }
        } catch (ConnectorPropertyException e) {
            factory = getApiClientFactory(); // I guess the default is something other than preview?
        }

        BatchProcessor processor = new BatchProcessor(factory);
        try {
            processor.setGeneratePasswords(config.getGeneratePasswords());
            if (!Strings.isNullOrEmpty(config.getExportPassword())) {
                processor.setExportPassword(config.getExportPassword());
            }
            Operation defaultOperation = config.getDefaultOperation();
            if (defaultOperation != null) {
                processor.setDefaultOperation(defaultOperation);
            }
            String template = config.getTemplate();
            if (!Strings.isNullOrEmpty(template)) {
                processor.setTemplate(template);
            }
            processor.setOutputFormat(config.getOutputFormat());
            String outputTemplate = config.getOutputTemplate();
            if (!Strings.isNullOrEmpty(outputTemplate)) {
                processor.setOutputTemplate(outputTemplate);
            }
        } catch (ConnectorPropertyException ignore) {}

        String content = bytes.toString();
        logger.debug(content);
        processor.processFile(path.getFileName().toString(), content);

        Json.mapper.writeValue(result.toFile(), processor.calculateResults());
    }

    private Path unique(Path parent, String base, String suffix) {
        String name = base + Strings.nullToEmpty(suffix);
        int counter = 0;
        while (Files.exists(parent.resolve(name))) {
            counter++;
            name = base+"("+counter+")"+Strings.nullToEmpty(suffix);
        }
        return parent.resolve(name);
    }

    @Override
    public void close() throws IOException {
        super.close();

        String name = path.getFileName().toString().replaceAll("(?i)\\.(ya?ml|csv|txt)$", "");
        Path result = unique(path.getParent(), name, ".result.yaml");
        logger.debug("generating "+result.getFileName()+" from "+path.getFileName());
        process(result);
    }

}
