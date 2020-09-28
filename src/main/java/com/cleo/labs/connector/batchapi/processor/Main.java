package com.cleo.labs.connector.batchapi.processor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.cleo.labs.connector.batchapi.processor.BatchProcessor.Operation;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.OutputFormat;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Strings;

public class Main {

    private static Options getOptions() {
        Options options = new Options();

        options.addOption(Option.builder()
                .longOpt("help")
                .build());

        options.addOption(Option.builder()
                .longOpt("url")
                .desc("VersaLex url")
                .hasArg()
                .argName("URL")
                .required(false)
                .build());

        options.addOption(Option.builder("k")
                .longOpt("insecure")
                .desc("Disable https security checks")
                .required(false)
                .build());

        options.addOption(Option.builder("u")
                .longOpt("username")
                .desc("Username")
                .hasArg()
                .argName("USERNAME")
                .required(false)
                .build());

        options.addOption(Option.builder("p")
                .longOpt("password")
                .desc("Password")
                .hasArg()
                .argName("PASSWORD")
                .required(false)
                .build());

        options.addOption(Option.builder("i")
                .longOpt("input")
                .desc("input file YAML, JSON or CSV")
                .hasArg()
                .argName("FILE")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("generate-pass")
                .desc("Generate Passwords for users")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("export-pass")
                .desc("Password to encrypt generated passwords")
                .hasArg()
                .argName("PASSWORD")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("operation")
                .hasArg()
                .argName("OPERATION")
                .desc("default operation: list, add, update, delete or preview")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("output-format")
                .hasArg()
                .argName("FORMAT")
                .desc("output format: yaml (default), json, or csv")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("output-template")
                .hasArg()
                .argName("TEMPLATE")
                .desc("template for formatting csv output")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("include-defaults")
                .desc("include all default values when listing connections")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("template")
                .hasArg()
                .argName("TEMPLATE")
                .desc("load CSV file using provided template")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("profile")
                .desc("Connection profile to use")
                .hasArg()
                .argName("PROFILE")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("save")
                .desc("Save/update profile")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("remove")
                .desc("Remove profile")
                .required(false)
                .build());

        options.addOption(Option.builder()
                .longOpt("trace-requests")
                .desc("include all default values when listing connections")
                .required(false)
                .desc("dump requests to stderr as a debugging aid")
                .build());

        return options;
    }

    public static void checkHelp(CommandLine cmd) {
        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(80);
            formatter.setOptionComparator(null);
            formatter.printHelp("com.cleo.labs.connector.batchapi.processor.Main", getOptions());
            System.exit(0);
        }
    }

    public static Profile loadProfile(String name, boolean quiet) {
        TypeReference<Map<String, Profile>> typeRef = new TypeReference<Map<String, Profile>>() {};
        Path cic = Paths.get(System.getProperty("user.home"), ".cic");
        Path filename = cic.resolve("profiles");
        try {
            Map<String, Profile> profiles = Json.mapper.readValue(filename.toFile(), typeRef);
            return profiles.get(name);
        } catch (JsonParseException | JsonMappingException e) {
            System.err.println("error parsing file "+filename+": "+ e.getMessage());
        } catch (IOException e) {
            if (!quiet) {
                System.err.println("error loading file "+filename+": " + e.getMessage());
            }
        }
        return null;
    }

    public static void removeProfile(String name, Profile profile) {
        TypeReference<Map<String, Profile>> typeRef = new TypeReference<Map<String, Profile>>() {};
        Path cic = Paths.get(System.getProperty("user.home"), ".cic");
        Path filename = cic.resolve("profiles");
        try {
            File file = filename.toFile();
            Map<String, Profile> profiles;
            try {
                profiles = Json.mapper.readValue(file, typeRef);
            } catch (IOException e) {
                System.err.println(filename+" not found while removing profile "+name+": "+e.getMessage());
                return; // no file, nothing to remove
            }
            if (!profiles.containsKey(name)) {
                System.err.println("profile "+name+" not found in "+filename);
                return; // nothing to remove
            }
            profiles.remove(name);
            Json.mapper.writeValue(filename.toFile(), profiles);
        } catch (JsonParseException | JsonMappingException e) {
            System.err.println("error parsing file "+filename+": " + e.getMessage());
        } catch (IOException e) {
            System.err.println("error updating file "+filename+": " + e.getMessage());
        }
    }

    public static void saveProfile(String name, Profile profile) {
        TypeReference<Map<String, Profile>> typeRef = new TypeReference<Map<String, Profile>>() {};
        Path cic = Paths.get(System.getProperty("user.home"), ".cic");
        Path filename = cic.resolve("profiles");
        if (!cic.toFile().isDirectory()) {
            cic.toFile().mkdir();
        }
        try {
            File file = filename.toFile();
            Map<String, Profile> profiles;
            try {
                profiles = Json.mapper.readValue(file, typeRef);
            } catch (IOException e) {
                profiles = new HashMap<>();
            }
            profiles.put(name, profile);
            Json.mapper.writeValue(filename.toFile(), profiles);
        } catch (JsonParseException | JsonMappingException e) {
            System.err.println("error parsing file "+filename+": " + e.getMessage());
        } catch (IOException e) {
            System.err.println("error updating file "+filename+": " + e.getMessage());
        }
    }

    public static Profile processProfileOptions(CommandLine cmd) throws Exception {
        Profile profile = null;
        List<String> missing = new ArrayList<>();
        profile = loadProfile(cmd.getOptionValue("profile", "default"),
                !cmd.hasOption("profile") || cmd.hasOption("remove") || cmd.hasOption("save"));
        if (profile == null) {
            profile = new Profile();
        }
        if (cmd.hasOption("url")) {
            profile.setUrl(cmd.getOptionValue("url"));
        }
        if (cmd.hasOption("insecure")) {
            profile.setInsecure(true);
        }
        if (cmd.hasOption("username")) {
            profile.setUsername(cmd.getOptionValue("username"));
        }
        if (cmd.hasOption("password")) {
            profile.setPassword(cmd.getOptionValue("password"));
        }
        if (cmd.hasOption("export-pass")) {
            profile.setExportPassword(cmd.getOptionValue("export-pass"));
        }
        if (Strings.isNullOrEmpty(profile.getUrl())) {
            missing.add("url");
        }
        if (Strings.isNullOrEmpty(profile.getUsername())) {
            missing.add("username (u)");
        }
        if (Strings.isNullOrEmpty(profile.getPassword())) {
            missing.add("password (p)");
        }
        if (cmd.hasOption("remove")) {
            removeProfile(cmd.getOptionValue("profile", "default"), profile);
        }
        if (cmd.hasOption("save")) {
            if (!missing.isEmpty()) {
                throw new Exception("Missing required options for --save: "
                    + missing.stream().collect(Collectors.joining(", ")));
            }
            saveProfile(cmd.getOptionValue("profile", "default"), profile);
        }
        if (!missing.isEmpty() && cmd.hasOption("input")) {
            throw new Exception("Missing required options or profile values: "
                    + missing.stream().collect(Collectors.joining(", ")));
        }
        return profile;
    }

    public static void main(String[] args) throws IOException {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        Profile profile = null;
        BatchProcessor.Operation operation = null;
        BatchProcessor.OutputFormat outputFormat = null;
        String outputTemplate = null;
        try {
            Options options = getOptions();
            cmd = parser.parse(options, args);
            checkHelp(cmd);
            profile = processProfileOptions(cmd);
            if (cmd.hasOption("operation")) {
                operation = Operation.valueOf(cmd.getOptionValue("operation"));
            }
            if (cmd.hasOption("output-format")) {
                outputFormat = OutputFormat.valueOf(cmd.getOptionValue("output-format"));
                if (outputFormat == OutputFormat.csv) {
                    if (!cmd.hasOption("output-template")) {
                        throw new Exception("output-template is required when output-format is csv");
                    }
                    outputTemplate = cmd.getOptionValue("output-template");
                }
            }
        } catch (Exception e) {
            System.err.println("Could not parse command line arguments: " + e.getMessage());
            System.exit(-1);
        }

        if (cmd.hasOption("input")) {
            REST restClient = null;
            if (operation != Operation.preview) {
                try {
                    restClient = new REST(profile.getUrl(), profile.getUsername(), profile.getPassword(), profile.isInsecure());
                    restClient.includeDefaults(cmd.hasOption("include-defaults"));
                    restClient.traceRequests(cmd.hasOption("trace-requests"));
                } catch (Exception e) {
                    System.err.println("Failed to create REST Client: " + e.getMessage());
                    System.exit(-1);
                }
            }
            BatchProcessor processor = new BatchProcessor(restClient)
                .setGeneratePasswords(cmd.hasOption("generate-pass"))
                .setExportPassword(profile.getExportPassword())
                .setDefaultOperation(operation)
                .setTraceRequests(cmd.hasOption("trace-requests"))
                .setOutputFormat(outputFormat);
            if (cmd.hasOption("template")) {
                processor.setTemplate(Paths.get(cmd.getOptionValue("template")));
            }
            if (outputTemplate != null) {
                processor.setOutputTemplate(Paths.get(outputTemplate));
            }
            processor.processFiles(cmd.getOptionValues("input"));
        }
    }

}
