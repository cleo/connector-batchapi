package com.cleo.labs.connector.batchapi;

import java.io.IOException;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;

import com.cleo.connector.api.directory.Directory;
import com.cleo.connector.api.directory.Directory.Type;

/**
 * File attribute view for a synthetic file with properties from the constructor.
 */
public class SyntheticFileAttributes implements DosFileAttributeView {
    private String name;
    private DosFileAttributes attrs;

    public SyntheticFileAttributes(String name, long size, FileTime t, Directory.Type type) {
        this.name = name;
        FileTime time = t == null ? FileTime.fromMillis(System.currentTimeMillis()) : t;
        this.attrs = new DosFileAttributes() {
            @Override
            public long size() {
                return size;
            }
            @Override
            public FileTime lastModifiedTime() {
                return time;
            }
            @Override
            public FileTime lastAccessTime() {
                return time;
            }
            @Override
            public boolean isSymbolicLink() {
                return false;
            }
            @Override
            public boolean isRegularFile() {
                return type != Type.dir;
            }
            @Override
            public boolean isOther() {
                return false;
            }
            @Override
            public boolean isDirectory() {
                return type == Type.dir;
            }
            @Override
            public Object fileKey() {
                return null;
            }
            @Override
            public FileTime creationTime() {
                return time;
            }
            @Override
            public boolean isSystem() {
                return false;
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
        };
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
