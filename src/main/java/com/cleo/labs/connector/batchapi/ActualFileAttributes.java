package com.cleo.labs.connector.batchapi;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;

import com.cleo.connector.api.ConnectorException;

/**
 * File Attribute View for an actual file.
 */
public class ActualFileAttributes implements DosFileAttributeView {
    private String name;
    private DosFileAttributes attrs;

    public ActualFileAttributes(Path path) throws IOException, ConnectorException {
        this.name = path.getFileName().toString();;
        DosFileAttributeView view = Files.getFileAttributeView(path, DosFileAttributeView.class);
        if (view == null) {
            throw new IOException("can not get DosFileAttributeView for "+path.toString());
        }
        try {
            this.attrs = view.readAttributes();
        } catch (NoSuchFileException e) {
            throw new ConnectorException(String.format("'%s' does not exist or is not accessible", path),
                                        ConnectorException.Category.fileNonExistentOrNoAccess);

        }
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        if (lastModifiedTime != null || lastAccessTime != null || createTime != null) {
            throw new UnsupportedOperationException("setTimes() not supported");
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DosFileAttributes readAttributes() throws IOException {
        return attrs;
    }

    @Override
    public void setReadOnly(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported");
    }

    @Override
    public void setHidden(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported");
    }

    @Override
    public void setSystem(boolean value) throws IOException {
        throw new UnsupportedOperationException("setSystem() not supported");
    }

    @Override
    public void setArchive(boolean value) throws IOException {
        throw new UnsupportedOperationException("setArchive() not supported");
    }

}
