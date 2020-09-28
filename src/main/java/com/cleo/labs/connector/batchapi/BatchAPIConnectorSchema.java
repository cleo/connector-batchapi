package com.cleo.labs.connector.batchapi;

import static com.cleo.connector.api.property.CommonPropertyGroups.Connect;
import static com.cleo.connector.api.property.CommonPropertyGroups.ConnectSecurity;

import java.io.IOException;

import com.cleo.connector.api.ConnectorConfig;
import com.cleo.connector.api.annotations.Client;
import com.cleo.connector.api.annotations.Connector;
import com.cleo.connector.api.annotations.ExcludeType;
import com.cleo.connector.api.annotations.Info;
import com.cleo.connector.api.annotations.Property;
import com.cleo.connector.api.interfaces.IConnectorProperty;
import com.cleo.connector.api.property.CommonProperties;
import com.cleo.connector.api.property.CommonProperty;
import com.cleo.connector.api.property.PropertyBuilder;
import com.cleo.connector.common.ConfigFileImport;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.Operation;
import com.cleo.labs.connector.batchapi.processor.BatchProcessor.OutputFormat;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

@Connector(scheme = "BatchAPI", description = "Harmony API Batch Processor",
           excludeType = { @ExcludeType(type = ExcludeType.SentReceivedBoxes),
                           @ExcludeType(type = ExcludeType.Exchange) })
@Client(BatchAPIConnectorClient.class)
public class BatchAPIConnectorSchema extends ConnectorConfig {
    @Property
    final IConnectorProperty<String> workingDirectory = new PropertyBuilder<>("WorkingDirectory", "")
            .setRequired(true)
            .setDescription("The working directory where request and reponse files are stored.")
            .setGroup(Connect)
            .setType(IConnectorProperty.Type.PathType)
            .build();

    @Property
    final IConnectorProperty<String> url = new PropertyBuilder<>("Url", "")
            .setRequired(true)
            .setDescription("The Harmony server URL including protocol, "+
                            "host and port, e.g. \"https://harmony.example.com:6080\".")
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<String> user = new PropertyBuilder<>("User", "")
            .setRequired(true)
            .setDescription("The Harmony API user.")
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<String> password = new PropertyBuilder<>("Password", "")
            .setRequired(true)
            .setDescription("The Harmony API password.")
            .addAttribute(IConnectorProperty.Attribute.Password)
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<Boolean> generatePasswords = new PropertyBuilder<>("GeneratePasswords", false)
            .setDescription("Select to generate passwords for created users and append the generated "+
                            "passwords, possibly encrypted, to the result file.")
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<String> exportPassword = new PropertyBuilder<>("ExportPassword", "")
            .setRequired(false)
            .setDescription("For generated passwords, the master password used to encrypt "+
                            "the passwords in the report appended to the result file.")
            .addAttribute(IConnectorProperty.Attribute.Password)
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<String> defaultOperation = new PropertyBuilder<>("DefaultOperation", "")
            .setRequired(false)
            .setDescription("The default operation to use for request entries without an explicit operation")
            .setAllowedInSetCommand(true)
            .addPossibleValues("",
                    Operation.list.name(),
                    Operation.add.name(),
                    Operation.update.name(),
                    Operation.delete.name(),
                    Operation.preview.name(),
                    Operation.run.name())
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<String> template = new PropertyBuilder<>("Template", "")
            .setDescription("Explicit template to use when processing CSV request files")
            .setGroup(Connect)
            .setExtendedClass(ConfigFileImport.class)
            .setRequired(false)
            .build();

    @Property
    final IConnectorProperty<String> outputFormat = new PropertyBuilder<>("OutputFormat", "")
            .setRequired(false)
            .setDescription("The output format for the result files (YAML by default)")
            .setAllowedInSetCommand(true)
            .addPossibleValues("",
                    OutputFormat.yaml.name(),
                    OutputFormat.json.name(),
                    OutputFormat.csv.name())
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<String> outputTemplate = new PropertyBuilder<>("OutputTemplate", "")
            .setDescription("Template to use when formatting CSV response files")
            .setGroup(Connect)
            .setExtendedClass(ConfigFileImport.class)
            .setRequired(false)
            .build();

    @Property
    final IConnectorProperty<Boolean> ignoreTLSChecks = new PropertyBuilder<>("IgnoreTLSChecks", false)
            .setDescription("Select to ignore TLS checks on trusted certificates and hostname matching.")
            .setGroup(ConnectSecurity)
            .build();

    @Property
    final IConnectorProperty<Boolean> enableDebug = CommonProperties.of(CommonProperty.EnableDebug);

    @Info
    protected static String info() throws IOException {
        return Resources.toString(BatchAPIConnectorSchema.class.getResource("info.txt"), Charsets.UTF_8);
    }
}
