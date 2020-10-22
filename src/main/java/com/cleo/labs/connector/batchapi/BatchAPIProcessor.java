package com.cleo.labs.connector.batchapi;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.cleo.connector.api.helper.Logger;
import com.cleo.connector.api.property.ConnectorPropertyException;
import com.cleo.labs.connector.batchapi.processor.ApiClient;
import com.cleo.labs.connector.batchapi.processor.ApiClientFactory;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.Operation;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.OutputFormat;
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
                Profile selected = null;     // set if we get a match by name
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

    private void process(Path outputFile, Path logFile) throws IOException {
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
            if (logFile != null) {
                processor.setLogOutput(logFile);
            }
        } catch (ConnectorPropertyException ignore) {}

        String content = bytes.toString();
        try (PrintStream out = new PrintStream(outputFile.toFile())) {
            processor.processFile(path.getFileName().toString(), content, out);
        }
    }

    private String unique(Path parent, String base, String suffix1, String suffix2) {
        suffix1 = Strings.nullToEmpty(suffix1);
        suffix2 = Strings.nullToEmpty(suffix2);
        String test = base;
        int counter = 0;
        while (Files.exists(parent.resolve(test+suffix1)) || Files.exists(parent.resolve(test+suffix2))) {
            counter++;
            test = base+"("+counter+")";
        }
        return test;
    }

    @Override
    public void close() throws IOException {
        super.close();

        String name = path.getFileName().toString();
        String base = name.replaceFirst("\\.[^.]*$","");
        OutputFormat outputFormat;
        try {
            if (!Strings.isNullOrEmpty(config.getOutputTemplate())) {
                outputFormat = OutputFormat.csv;
            } else {
                outputFormat = config.getOutputFormat();
            }
        } catch (ConnectorPropertyException e) {
            outputFormat = OutputFormat.yaml;
        }
        String ext = "."+outputFormat.name();
        String log = outputFormat == OutputFormat.csv ? ".log" : null;
        Path parent = path.getParent();
        String unique = unique(parent, base, ext, log);
        Path outputFile = parent.resolve(unique+ext);
        Path logFile = null;
        logger.debug("generating "+outputFile.getFileName()+" from "+path.getFileName());
        if (outputFormat == OutputFormat.csv) {
            logFile = outputFormat == OutputFormat.csv ? parent.resolve(unique+log) : null;
            logger.debug("logging to "+logFile+" from "+path.getFileName());
        }
        process(outputFile, logFile);
    }

}
