package com.cleo.labs.connector.batchapi.processor;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
            System.arraycopy(header, BEGIN_BYTES.length, trailer, END_BYTES.length, trailer.length-END_BYTES.length);
            return trailer;
        }
        return EMPTY;
    }

    private static boolean matchesAt(byte[] buffer, int offset, byte[] prefix) {
        boolean result = false;
        if (buffer.length-offset >= prefix.length) {
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

    /**
     * Parses a byte buffer in one of the following formats,
     * returning a list of binary bytes buffers depending on
     * what was found:
     * <ul><li>a base64-encoded sequence</li>
     *     <li>a base64-encoded sequence enclosed in BEGIN and END delimiters</li>
     *     <li>a list of base64-encoded sequences enclosed in BEGIN and END delimiters</li>
     *     <li>the original buffer unchanged</li>
     * </ul>
     * For the three base64 formats, the decoded buffer is returned.
     * For the third list option, multiple decoded buffers are returned.
     * @param buffer an input buffer, possibly in a base64 encoded format
     * @return a list of 1 or more binary buffers, possibly base64 decoded
     */
    public static List<byte[]> bytes(byte[] buffer) {
        byte[] header = findHeader(buffer);
        byte[] trailer = trailer(header);
        boolean foundTrailer = false;
        boolean base64 = true;
        int start = header.length;
        int i = start;
        int size = 0;
        List<byte[]> results = new ArrayList<>();
        while (base64 && i < buffer.length) {
            while (base64 && !foundTrailer && i < buffer.length) {
                if (BASE64_CHARS.contains(buffer[i])) {
                    if (!EOL_CHARS.contains(buffer[i])) {
                        size++;
                    }
                    i++;
                } else if (header.length>0 && matchesAt(buffer, i, trailer)) {
                    foundTrailer = true;
                    i += trailer.length;
                    while (i < buffer.length && EOL_CHARS.contains(buffer[i])) {
                        i++;
                    }
                    if (matchesAt(buffer, i, header)) {
                        i += header.length;
                    }
                } else {
                    base64 = false;
                }
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
                    results.add(Base64.getDecoder().decode(result));
                } catch (Exception e) {
                    // fall through and return the original
                    e.printStackTrace();
                }
                foundTrailer = false;
                size = 0;
                start = i;
            }
        }
        if (results.isEmpty()) {
            results.add(buffer); // didn't find any base64 so return original
        }
        return results;
    }

    /**
     * Analyzes a byte buffer and tries to parse X509Certificates from it.
     * The following are accepted:
     * <ul><li>a binary certificate (as might be returned by
     *         {@link X509Certificate#getEncoded()}</li>
     *     <li>a base 64 encoded certificate (as might be returned by the Harmony API></li>
     *     <li>a base 64 encoded certificate with BEGIN and END headers and folded lines,
     *         (as might be produced by a tool such as {@code openssl}</li>
     *     <li>a list of base 64 encoded certificates enclosed in BEGIN/END headers</li>
     *     <li>a binary {@code .p7b} file (a PKCS#7 file with a cert bundle)&mdash;
     *         the first end-entity certificate encountered will be returned from
     *         the bundle</li>
     *  </ul>
     * @param buffer the buffer to parse
     * @return a list of certificates
     * @throws Exception if any of the buffers won't parse as a cert or PKCS#7
     */
    public static List<X509Certificate> certs(byte[] buffer) throws Exception {
        List<byte[]> binaries = bytes(buffer);
        List<X509Certificate> certs = new ArrayList<>();
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        // may throw a CertificateException, but won't
        for (byte[] binary : binaries) {
            try (InputStream is = new ByteArrayInputStream(binary)) {
                certs.add((X509Certificate)cf.generateCertificate(is));
            } catch (Exception e) {
                // try p7b instead
                try (InputStream is = new ByteArrayInputStream(binary)) {
                    PKCS7CertList list = new PKCS7CertList(is);
                    certs.addAll(Arrays.asList(list.getCertificateList()));
                    // may throw a PKCSParsingException
                }
            }
        }
        return certs;
    }

    public static class CertWithBundle {
        private X509Certificate cert;
        private List<X509Certificate> bundle;
        public CertWithBundle() {
            this.cert = null;
            this.bundle = new ArrayList<>();
        }
        public CertWithBundle cert(X509Certificate cert) {
            this.cert = cert;
            return this;
        }
        public CertWithBundle bundling(X509Certificate cert) {
            this.bundle.add(cert);
            return this;
        }
        public X509Certificate cert() {
            return cert;
        }
        public List<X509Certificate> bundle() {
            return bundle;
        }
        public static CertWithBundle of(X509Certificate cert) {
            return new CertWithBundle().cert(cert);
        }
    }

    /**
     * Analyzes a byte buffer, tries to parse X509Certificates from it
     * (see {@link #certs}), isolating an end-entity certificate into
     * a {@link CertWithBundle} with any remaining certificates in
     * the bundle.
     * @param buffer the buffer to parse
     * @return a CertWithBundle
     * @throws Exception if any of the buffers won't parse as a cert or PKCS#7
     */
    public static CertWithBundle cert(byte[] buffer) throws Exception {
        List<X509Certificate> certs = certs(buffer);
        if (certs.size() <= 1) { // but it can't be 0
            return CertWithBundle.of(certs.get(0));
        }

        // if multiple certs, eliminate the referenced issuers and return the first EE cert
        Set<String> issuers = certs.stream().map(c -> c.getIssuerX500Principal().getName()).collect(Collectors.toSet());
        CertWithBundle result = new CertWithBundle();
        for (X509Certificate cert : certs) {
            if (result.cert()== null && !issuers.contains(cert.getSubjectX500Principal().getName())) {
                result.cert(cert);
            } else {
                result.bundling(cert);
            }
        }

        // if they were all referenced issuers, just return the first one
        if (result.cert() == null) {
            result.cert(result.bundle().remove(0));
        }

        return result;
    }

    /**
     * Analyzes the contents of a file indicated by a path name as
     * described for {@link #cert(byte[])}.
     * @param path the path of the file to read
     * @return a CertWithBundle
     * @throws Exception if any of the buffers won't parse as a cert or PKCS#7 or there's an IOException
     */
    public static CertWithBundle cert(Path path) throws Exception {
        byte[] buffer;
        try (InputStream is = new FileInputStream(path.toFile())) {
            buffer = ByteStreams.toByteArray(is);
            return cert(buffer);
        }
    }

    /**
     * Analyzes the contents of a String as described for {@link #cert(byte[])}.
     * @param s the String to analyze
     * @return an Optional<X509Certificate>
     * @throws Exception if any of the buffers won't parse as a cert or PKCS#7
     */
    public static CertWithBundle cert(String s) throws Exception {
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
