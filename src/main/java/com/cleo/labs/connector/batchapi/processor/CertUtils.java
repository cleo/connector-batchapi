package com.cleo.labs.connector.batchapi.processor;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;

import iaik.pkcs.PKCS7CertList;

public class CertUtils {

    private static final String BEGIN = "-----BEGIN ";
    private static final byte[] BEGIN_BYTES = BEGIN.getBytes();
    private static final String DASH = "-----";
    private static final byte[] DASH_BYTES = DASH.getBytes();
    private static final String END = "-----END ";
    private static final byte[] END_BYTES = END.getBytes();

    private static final byte[] EMPTY = new byte[0];

    private static byte[] findHeader(byte[] buffer) {
        if (matchesAt(buffer, 0, BEGIN_BYTES)) {
            for (int i=BEGIN_BYTES.length; i<buffer.length; i++) {
                if (matchesAt(buffer, i, DASH_BYTES)) {
                    byte[] header = new byte[i+DASH_BYTES.length];
                    System.arraycopy(buffer, 0, header, 0, header.length);
                    return header;
                }
            }
        }
        return EMPTY;
    }

    private static byte[] trailer(byte[] header) {
        if (header.length > 0) {
            byte[] trailer = new byte[header.length-(BEGIN_BYTES.length-END_BYTES.length)];
            System.arraycopy(END_BYTES, 0, trailer, 0, END_BYTES.length);
            System.arraycopy(header, BEGIN_BYTES.length, trailer, END_BYTES.length, trailer.length-BEGIN_BYTES.length);
            return trailer;
        }
        return EMPTY;
    }

    private static boolean matchesAt(byte[] buffer, int offset, byte[] prefix) {
        boolean result = false;
        if (offset+buffer.length >= prefix.length) {
            result = true;
            for (int i=0; result & i<prefix.length; i++) {
                if (buffer[offset+i] != prefix[i]) {
                    result = false;
                }
            }
        }
        return result;
    }

    private static Set<Byte> BASE64_CHARS =
        string2set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/=\r\n");
    private static Set<Byte> EOL_CHARS = string2set("\r\n");

    private static Set<Byte> string2set(String s) {
        Set<Byte> result = new HashSet<>();
        for (int i=0; i<s.length(); i++) {
            result.add(Byte.valueOf((byte)(s.codePointAt(i) & 0xFF)));
        }
        return result;
    }

    private static byte[] bytes(byte[] buffer) {
        byte[] header = findHeader(buffer);
        byte[] trailer = trailer(header);
        boolean foundTrailer = false;
        boolean base64 = true;
        int start = header.length;
        int i = 0;
        int size = 0;
        while (base64 && !foundTrailer && i < buffer.length) {
            if (BASE64_CHARS.contains(buffer[i])) {
                if (!EOL_CHARS.contains(buffer[i])) {
                    size++;
                }
            } else if (header.length>0 && matchesAt(buffer, i, trailer)) {
                foundTrailer = true;
            } else {
                base64 = false;
            }
            i++;
        }
        if (base64 && (header.length==0 || foundTrailer) && size > 0) {
            byte[] result = new byte[size];
            int k=0;
            for (int j=start; k < size; j++) {
                if (BASE64_CHARS.contains(buffer[j]) && !EOL_CHARS.contains(buffer[j])) {
                    result[k++] = buffer[j];
                }
            }
            try {
                return Base64.getDecoder().decode(result);
            } catch (Exception e) {
                // fall through and return the original
            }
        }
        return buffer;
    }

    /**
     * Analyzes a byte buffer and tries to parse an X509Certificate from it.
     * The following are accepted:
     * <ul><li>a binary certificate (as might be returned by
     *         {@link X509Certificate#getEncoded()}</li>
     *     <li>a base 64 encoded certificate (as might be returned by the Harmony API></li>
     *     <li>a base 64 encoded certificate with BEGIN and END headers and folded lines,
     *         (as might be produced by a tool such as {@code openssl}</li>
     *     <li>a binary {@code .p7b} file (a PKCS#7 file with a cert bundle)&mdash;
     *         the first end-entity certificate encountered will be returned from
     *         the bundle</li>
     *  </ul>
     * @param buffer the buffer to parse
     * @return an X509Certificate, or {@code null} if none of the above are recognized
     */
    public static X509Certificate cert(byte[] buffer) {
        buffer = bytes(buffer);
        try (InputStream is = new ByteArrayInputStream(buffer)) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            return (X509Certificate)cf.generateCertificate(is);
        } catch (Exception e) {
            // ignore -- try p7b
        }
        try (InputStream is = new ByteArrayInputStream(buffer)) {
            PKCS7CertList list = new PKCS7CertList(is);
            X509Certificate[] certs = list.getCertificateList();
            Set<String> issuers = Stream.of(certs).map(c -> c.getIssuerX500Principal().getName()).collect(Collectors.toSet());
            X509Certificate[] eeCerts = Stream.of(certs).filter(c -> !issuers.contains(c.getSubjectX500Principal().getName())).toArray(X509Certificate[]::new);
            return eeCerts[0];
        } catch (Exception e) {
            e.printStackTrace();
            // out of ideas -- fall through to return null
        }
        return null;
    }

    /**
     * Analyzes the contents of a file indicated by a path name as
     * described for {@link #cert(byte[])}.
     * @param path the path of the file to read
     * @return an X509Certificate, or {@code null}
     */
    public static X509Certificate cert(Path path) {
        byte[] buffer;
        try (InputStream is = new FileInputStream(path.toFile())) {
            buffer = ByteStreams.toByteArray(is);
            return cert(buffer);
        } catch (IOException e) {
            // fall through to return null
        }
        return null;
    }

    /**
     * Analyzes the contents of a String as described for {@link #cert(byte[])}.
     * @param s the String to analyze
     * @return an X509Certificate, or {@code null}
     */
    public static X509Certificate cert(String s) {
        return cert(s.getBytes());
    }

    /**
     * Returns the base 64 encoding of a certificate, or
     * {@code null} if there is an encoding error.
     * @param cert the certificate to encode
     * @return a base 64 string, or {@code null}
     */
    public static String base64(X509Certificate cert) {
        if (cert == null) {
            return null;
        }
        try {
            return Base64.getEncoder().encodeToString(cert.getEncoded());
        } catch (CertificateEncodingException e) {
            return null;
        }
    }

    /**
     * Converts a certificate into a multi-line base 64 encoded format
     * wrapped in BEGIN and END delimiters, such as might be returned
     * by {@code openssl}.
     * @param cert a (possibly {@code null}) X509Certificate
     * @return a multi-line String (or {@code null})
     */
    public static String export(X509Certificate cert) {
        return export(base64(cert));
    }

    /**
     * Wraps a base 64 encoded certificate in BEGIN and END delimiters,
     * such as might be returned by {@code openssl}.
     * @param cert a (possibly {@code null}) base 64 encoded string
     * @return a multi-line String (or {@code null})
     */
    public static String export(String cert) {
        if (Strings.isNullOrEmpty(cert)) {
            return null;
        }
        return BEGIN+"CERTIFICATE"+DASH+"\n"+
            OpenSSLCrypt.fold(cert)+"\n"+
            END+"CERTIFICATE"+DASH;
    }

    private CertUtils() {
    }
}
