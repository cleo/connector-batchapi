package com.cleo.labs.connector.batchapi;

import java.io.IOException;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * batch API file attribute views
 */
public class BatchAPIDirAttributes implements DosFileAttributes, DosFileAttributeView {
    BatchAPIConnectorConfig config;

    public BatchAPIDirAttributes(BatchAPIConnectorConfig config) {
        this.config = config;
    }

    @Override
    public FileTime lastModifiedTime() {
        return FileTime.from(new Date().getTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public FileTime lastAccessTime() {
        return FileTime.from(new Date().getTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public FileTime creationTime() {
        return FileTime.from(new Date().getTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isRegularFile() {
        return false;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return -1L;
    }

    @Override
    public Object fileKey() {
        return null;
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        if (lastModifiedTime != null || lastAccessTime != null || createTime != null) {
            throw new UnsupportedOperationException("setTimes() not supported for BatchAPI");
        }
    }

    @Override
    public String name() {
        return "random.txt";
    }

    @Override
    public DosFileAttributes readAttributes() throws IOException {
        return this;
    }

    @Override
    public void setReadOnly(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported for BatchAPI");
    }

    @Override
    public void setHidden(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported for BatchAPI");
    }

    @Override
    public void setSystem(boolean value) throws IOException {
        throw new UnsupportedOperationException("setSystem() not supported for BatchAPI");
    }

    @Override
    public void setArchive(boolean value) throws IOException {
        throw new UnsupportedOperationException("setArchive() not supported for BatchAPI");
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isArchive() {
        return false;
    }

    @Override
    public boolean isSystem() {
        return false;
    }

}
