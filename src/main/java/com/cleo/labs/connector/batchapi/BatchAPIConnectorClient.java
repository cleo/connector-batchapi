package com.cleo.labs.connector.batchapi;

import static com.cleo.connector.api.command.ConnectorCommandName.ATTR;
import static com.cleo.connector.api.command.ConnectorCommandName.DELETE;
import static com.cleo.connector.api.command.ConnectorCommandName.DIR;
import static com.cleo.connector.api.command.ConnectorCommandName.GET;
import static com.cleo.connector.api.command.ConnectorCommandName.PUT;
import static com.cleo.connector.api.command.ConnectorCommandName.RENAME;
import static com.cleo.connector.api.command.ConnectorCommandOption.Delete;
import static com.cleo.connector.api.command.ConnectorCommandOption.Unique;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.cleo.connector.api.ConnectorClient;
import com.cleo.connector.api.ConnectorException;
import com.cleo.connector.api.annotations.Command;
import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.DirCommand;
import com.cleo.connector.api.command.GetCommand;
import com.cleo.connector.api.command.OtherCommand;
import com.cleo.connector.api.command.PutCommand;
import com.cleo.connector.api.command.ConnectorCommandResult.Status;
import com.cleo.connector.api.directory.Directory.Type;
import com.cleo.connector.api.directory.Entry;
import com.cleo.connector.api.helper.Attributes;
import com.cleo.connector.api.interfaces.IConnectorIncoming;
import com.cleo.connector.api.interfaces.IConnectorOutgoing;
import com.cleo.connector.file.FileAttributes;

public class BatchAPIConnectorClient extends ConnectorClient {
    private BatchAPIConnectorConfig config;

    public BatchAPIConnectorClient(BatchAPIConnectorSchema schema) {
        this.config = new BatchAPIConnectorConfig(this, schema);
    }

    @Command(name = PUT, options = { Delete, Unique })
    public ConnectorCommandResult put(PutCommand put) throws ConnectorException {
        IConnectorOutgoing source = put.getSource();
        String destination = put.getDestination().getPath();

        logger.debug(String.format("PUT local '%s' to remote '%s'", source.getPath(), destination));

        try {
            BatchAPIProcessor processor = new BatchAPIProcessor(
                    config,
                    config.getWorkingDirectory().resolve(destination),
                    source.getMetadata(),
                    logger);
            transfer(source.getStream(), processor, false);
        } catch (Exception e) {
            return new ConnectorCommandResult(Status.Error, "Put failed.", e);
        }

        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name = GET, options = { Delete, Unique })
    public ConnectorCommandResult get(GetCommand get) throws ConnectorException, IOException {
        String source = get.getSource().getPath();
        IConnectorIncoming destination = get.getDestination();
        logger.debug(String.format("GET remote '%s' to local '%s'", source, destination.getPath()));

        try {
            Path path = config.getWorkingDirectory().resolve(source);
            if (!path.toFile().exists()) {
              throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path.toString()),
                      ConnectorException.Category.fileNonExistentOrNoAccess);
            }
            transfer(new FileInputStream(path.toFile()), destination.getStream(), true);
        } catch (Exception e) {
            return new ConnectorCommandResult(Status.Error, "Get failed.", e);
        }

        return new ConnectorCommandResult(ConnectorCommandResult.Status.Success);
    }

    @Command(name=DELETE)
    public ConnectorCommandResult delete(OtherCommand delete) throws ConnectorException {
        String relativepath = delete.getSource();
        logger.debug(String.format("DELETE '%s'", relativepath));

        try {
            Path path = config.getWorkingDirectory().resolve(relativepath);
            File file = path.toFile();
            if (!file.exists()) {
                throw new ConnectorException(String.format("'%s' does not exist or is not accessible", relativepath),
                        ConnectorException.Category.fileNonExistentOrNoAccess);
            } else if (!file.isFile()) {
                return new ConnectorCommandResult(Status.Error, String.format("'%s' is not a file", relativepath));
            } else if (!file.delete()) {
                return new ConnectorCommandResult(Status.Error, "Delete failed.");
            } else {
                return new ConnectorCommandResult(Status.Success);
            }
        } catch (Exception e) {
            return new ConnectorCommandResult(Status.Error, "Delete failed.", e);
        }
    }

    @Command(name=RENAME)
    public ConnectorCommandResult rename(OtherCommand rename) throws ConnectorException {
        String relativesource = rename.getSource();
        String relativedestination = rename.getDestination();
        logger.debug(String.format("RENAME '%s' '%s'", relativesource, relativedestination));

        try {
            Path path = config.getWorkingDirectory().resolve(relativesource);
            File file = path.toFile();
            if (!file.exists()) {
                throw new ConnectorException(String.format("'%s' does not exist or is not accessible", file.getPath()),
                        ConnectorException.Category.fileNonExistentOrNoAccess);
            } else if (!file.renameTo(config.getWorkingDirectory().resolve(relativedestination).toFile())) {
                return new ConnectorCommandResult(Status.Error, "Rename failed.");
            } else {
                return new ConnectorCommandResult(Status.Success);
            }
        } catch (Exception e) {
            return new ConnectorCommandResult(Status.Error, "Rename failed.", e);
        }
    }

    @Command(name = DIR)
    public ConnectorCommandResult dir(DirCommand dir) throws ConnectorException {
        String relativepath = dir.getSource().getPath();
        logger.debug(String.format("DIR '%s'", relativepath));

        try {
            Path path = config.getWorkingDirectory().resolve(relativepath);
            List<Entry> result = new ArrayList<>();
            File[] files = path.toFile().listFiles();
            if (files != null) {
                for (File file : files) {
                    Entry entry = new Entry(file.isFile() ? Type.file : Type.dir);
                    entry.setPath(Paths.get(relativepath, file.getName()).toString());
                    entry.setDate(Attributes.toLocalDateTime(file.lastModified()));
                    entry.setSize(file.isFile() ? file.length() : -1L);
                    result.add(entry);
                }
            }
            return new ConnectorCommandResult(ConnectorCommandResult.Status.Success, Optional.empty(), result);
        } catch (Exception e) {
            return new ConnectorCommandResult(Status.Error, "Dir failed.", e);
        }
    }

    /**
     * Get the file attribute view associated with a file path
     * 
     * @param path the file path
     * @return the file attributes
     * @throws com.cleo.connector.api.ConnectorException
     * @throws java.io.IOException
     */
    @Command(name = ATTR)
    public BasicFileAttributeView getAttributes(String path) throws ConnectorException, IOException {
        try {
            Path rootPath = config.getWorkingDirectory();
            Path fullPath = rootPath.resolve(path);
            File file = fullPath.toFile();
            if (file.exists()) {
                return new FileAttributes(file);
            }
        } catch (InvalidPathException ipEx) {
            // turn this exception into fileNonExistentOrNoAccess -- others will be thrown
        }
        throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                ConnectorException.Category.fileNonExistentOrNoAccess);
    }

}
