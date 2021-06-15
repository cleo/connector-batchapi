package com.cleo.labs.connector.batchapi;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

import com.cleo.connector.api.annotations.Display;
import com.cleo.connector.api.annotations.Setter;
import com.google.common.io.ByteStreams;

public class BinaryConfigFileImport {
    private static final DateFormat DATEFORMAT = new SimpleDateFormat("'Imported on' yyyy/MM/dd HH:mm:ss");
    public static final String DELIMITER = "@@@";

    @Setter("Import")
    public String importFile(InputStream in) throws IOException {
        return new StringBuilder(DATEFORMAT.format(new Date()))
                .append(DELIMITER)
                .append(Base64.getEncoder().encodeToString(ByteStreams.toByteArray(in)))
                .toString();
    }

    @Display
    public String display(String value) {
        if (value != null && value.contains(DELIMITER)) {
            value = value.substring(0, value.indexOf(DELIMITER));
        }
        return value;
    }

    public static byte[] value(String value) {
        if (value != null && value.contains(DELIMITER)) {
            return Base64.getDecoder().decode(
                    value.substring(value.indexOf(DELIMITER) + DELIMITER.length()));
        }
        return new byte[0];
    }

    public static String valueString(String value) {
        return new String(value(value));
    }

}
