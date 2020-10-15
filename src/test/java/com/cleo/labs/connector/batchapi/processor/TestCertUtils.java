package com.cleo.labs.connector.batchapi.processor;

import static org.junit.Assert.*;

import java.nio.file.Paths;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;

import org.junit.Test;

public class TestCertUtils {

    private static final byte[] c1 =
       ("MIICyjCCAbKgAwIBAgIGAXUikj7bMA0GCSqGSIb3DQEBBQUAMBwxCzAJBgNVBAYT"+
        "AnVzMQ0wCwYDVQQDEwR0ZXN0MB4XDTIwMTAxMzE1MjY1MVoXDTIyMTAxMzE1MjY1"+
        "MVowHDELMAkGA1UEBhMCdXMxDTALBgNVBAMTBHRlc3QwggEiMA0GCSqGSIb3DQEB"+
        "AQUAA4IBDwAwggEKAoIBAQCvL2/R7hXW03y3h2pZ1jNlQqtkXQATnwtDmzjYNwvz"+
        "zfQEWvRRTF1KtrRabw0vcYzrcmDGAPAbax1ahKvhLjV656EHTfl5HGSI4/bPy5rx"+
        "yoWDKGCvcllUOzFNZUsaXbz5Kir12Kse9lkF/4fayCs3cIzYHmM36gvG1F1Enxfn"+
        "0ugleJydvfR4ZRYgoHlqeVQWCPxLeW2FLQ+l9mN8Xok0KLG3QKr4avcnzEIfXdb1"+
        "9P6/uX4t32bqP+TwlYdoO5lvC2n+yTY4iB+VFja9BmfEBJKiUR8iQtRZeS5RI50g"+
        "vae0H/VLidRbubJUgbZwG0clJaLId+3mBPGzlQ2qhsgDAgMBAAGjEjAQMA4GA1Ud"+
        "DwEB/wQEAwIFoDANBgkqhkiG9w0BAQUFAAOCAQEAIm32FfFRUkelAv0z5/2x8bmk"+
        "+YZABdP7ht7mR6XdLWATojKAw/g0b2Ge50W763exK78vx9ZBqBYOXP08LipmXzqf"+
        "k7Wk1p20CD4nVv1kYu4vNbEGtcZJruI0/ybWxmg2GPMLF2pFnYo8H9Ww+2H9ycbJ"+
        "V1XYFT3Ent0QW3j+OYxhUKB3SkIq37H9Xw3TxfId5xTrosujQl5oZ0uALxxuXzsN"+
        "bwxYGdkKJkG5Hg7Hm1CfP0yxfSk9d8ZK9MvF4BUaALXNh3FqjfbiIqSHc2btoBIK"+
        "rtCCT+aA9/6R60X4cPrx81hK9m3nId/ou7b+e1QXzhM3Ah5N1u+nNhmFNjuunw==")
        .getBytes();

    private static final byte[] b1 = Base64.getDecoder().decode(c1);

    private static final byte[] c2 =
       ("-----BEGIN CERTIFICATE-----\n" + 
        "MIIDQzCCAiugAwIBAgIQLGG6JRAsQ4S5W1ZBieixJTANBgkqhkiG9w0BAQsFADAy\n" + 
        "MQswCQYDVQQGEwJVUzENMAsGA1UECgwEQ2xlbzEUMBIGA1UEAwwLRGVtbyBJc3N1\n" + 
        "ZXIwHhcNMjAxMDAxMDMyMDA4WhcNMjExMDAxMDMyMDA4WjAgMQswCQYDVQQGEwJV\n" + 
        "UzERMA8GA1UEAwwIU2lnbiAxMDUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK\n" + 
        "AoIBAQC2MqK1Z1Bg08tJdpo3S6iQ7vcsBFlK4OKdkYuhf9Wioy/J+oHmichzPxE5\n" + 
        "jOCC9gM1iDZ9X7reEUVDKyDb83wT2qj5cO0vw7jj8hVrmybRsJLRheYRsjC5HUyR\n" + 
        "gIU8rG5drkwbE0UDZXYSp41puotpsGwwnctdwczNolBiSlJnv844uGtawstOE7Su\n" + 
        "eWG8STWLDFcdx26lo45pllpbvE0u8t6MFzwpt8z5GSzjz5wksIANg1IcIruIdmvm\n" + 
        "f9CZ/qS8VpFwqvsPhWdXYZqjRquo4UMmTDA26IQOn+9jQEQ0toZn7AZPS4mU5v+X\n" + 
        "tbzHCMq7QbMKKe2i8SvsrbKoAnTVAgMBAAGjZzBlMAsGA1UdDwQEAwIFoDAWBgNV\n" + 
        "HSUBAf8EDDAKBggrBgEFBQcDATAdBgNVHQ4EFgQUeBrHJ1fNFMBlzZhmRPFtbVq1\n" + 
        "JAUwHwYDVR0jBBgwFoAU52GI/XKU1F8qd36/Z06p+mp4t9MwDQYJKoZIhvcNAQEL\n" + 
        "BQADggEBAEEIXPAysj6SsibGIPH0VWeADr0w5WvsxjqnLeCXLMwvsRPUKvUPPFGB\n" + 
        "KgfTHcBllZl7GriylJAnPy5FpHBgXxiTp6nn8had3yM6gA8sOjG4DntNhy/Tsh96\n" + 
        "KpUTeP63pMj6mhLfzAuWzEQLmIgQX88FIraXWESrmZcYnZy9sS/DPnMhtwkmGYxl\n" + 
        "UdgcTDbUUk7Pn5wAdNiNv7swFu1ig3SYgp21opqmBtEHmbOQranJjC+nFgejyrdt\n" + 
        "qJpNW5gIixoslRlr8OLnU3uAwiNBQgIZHSsnjybALw3bv+ChfEAGBPfVIXtCPETZ\n" + 
        "9OjeQgulu5t1XepHst0rnzk9N1BWH+0=\n" + 
        "-----END CERTIFICATE-----")
        .getBytes();

    private static final byte[] c3 = new String(c2).replaceAll("\n", "\r\n").getBytes();

    private static final byte[] c4 = new String(c2).replaceAll("CERTIFICATE", "PKCS7").getBytes();
    

    @Test
    public void testb1() throws Exception {
        X509Certificate cert = CertUtils.cert(b1);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }
    @Test
    public void testc1() throws Exception {
        X509Certificate cert = CertUtils.cert(c1);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }
    @Test
    public void testc2() throws Exception {
        X509Certificate cert = CertUtils.cert(c2);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }
    @Test
    public void testc3() throws Exception {
        assertNotEquals(c3, c2);
        X509Certificate cert = CertUtils.cert(c3);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }

    @Test
    public void testc4() throws Exception {
        assertNotEquals(c4, c2);
        X509Certificate cert = CertUtils.cert(c4);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }

    @Test
    public void test7() throws Exception {
        X509Certificate cert = CertUtils.cert(Paths.get("chain.p7b"));
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }
}