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
    
    private static final byte[] c5 =
       ("-----BEGIN CERTIFICATE-----\n" +
        "MIIGYDCCBUigAwIBAgIQAdJhYztbqb1npcEKg9d8jzANBgkqhkiG9w0BAQsFADBL\n" +
        "MQswCQYDVQQGEwJERTEZMBcGA1UEChMQRGV1dHNjaGUgUG9zdCBBRzEhMB8GA1UE\n" +
        "AxMYRFBESEwgR2xvYmFsIFRMUyBDQSAtIEk1MB4XDTIwMDczMDA5MjYzNFoXDTIx\n" +
        "MDczMDA5MjUzNFowdjELMAkGA1UEBhMCREUxHDAaBgNVBAgME05vcmRyaGVpbi1X\n" +
        "ZXN0ZmFsZW4xDTALBgNVBAcMBEJvbm4xGTAXBgNVBAoMEERldXRzY2hlIFBvc3Qg\n" +
        "QUcxHzAdBgNVBAMMFmN5Y2xvbmUzLWV1LXFhLmRobC5jb20wggEiMA0GCSqGSIb3\n" +
        "DQEBAQUAA4IBDwAwggEKAoIBAQC05zn4j4PZymB8wG/ANsZwv36qH5qInV9Z1wgb\n" +
        "p250GRdezt+WbwGlYWxienHuBnYH/Aiq2DIspFnlMTW1YFXh2cvrlrCFsyPEBQrv\n" +
        "oCYpy4H3LTrj55BX9GbbUAf1d+IwCM5iaiCZkbU5lSwU/c80weP/rHS7WwS7AOCh\n" +
        "dHezH4JhsPRiDWsxCZYf66L1dnSwZ9TVBNt4fGELPvMfyiBsLRkHBcspAaxazflA\n" +
        "b1fueVrK49IWN0zJQaS1QtuM8vj6f4G8LghdV9Jz7jNy4nSTKfCqd2VVfr+9jcGS\n" +
        "2gpqadGNFgFvGJFqY40DStRNBcbdyMIUz3gnKjxeu8Tkj1frAgMBAAGjggMTMIID\n" +
        "DzBvBgNVHREEaDBmghZjeWNsb25lMy1ldS1xYS5kaGwuY29tghZkc2MtcWEuYjJi\n" +
        "LnN5c3RlbXMuZGhsghlkc2MtcWEtYW0uYjJiLnN5c3RlbXMuZGhsghlkc2MtcWEt\n" +
        "ZWEuYjJiLnN5c3RlbXMuZGhsMA4GA1UdDwEB/wQEAwIFoDAdBgNVHSUEFjAUBggr\n" +
        "BgEFBQcDAQYIKwYBBQUHAwIwHQYDVR0OBBYEFEwjqG/Z00qIZnT53MUtvgkh/yZs\n" +
        "MFYGA1UdIARPME0wCAYGZ4EMAQICMEEGCSsGAQQBoDIBFDA0MDIGCCsGAQUFBwIB\n" +
        "FiZodHRwczovL3d3dy5nbG9iYWxzaWduLmNvbS9yZXBvc2l0b3J5LzAJBgNVHRME\n" +
        "AjAAMIGEBggrBgEFBQcBAQR4MHYwNAYIKwYBBQUHMAGGKGh0dHA6Ly9vY3NwLmds\n" +
        "b2JhbHNpZ24uY29tL2NhL2RobHRsc2NhaTUwPgYIKwYBBQUHMAKGMmh0dHA6Ly9z\n" +
        "ZWN1cmUuZ2xvYmFsc2lnbi5jb20vY2FjZXJ0L2RobHRsc2NhaTUuY3J0MB8GA1Ud\n" +
        "IwQYMBaAFO9b9EuG+db5OlOJCbwgjrwayiJgMDwGA1UdHwQ1MDMwMaAvoC2GK2h0\n" +
        "dHA6Ly9jcmwuZ2xvYmFsc2lnbi5jb20vY2EvZGhsdGxzY2FpNS5jcmwwggEDBgor\n" +
        "BgEEAdZ5AgQCBIH0BIHxAO8AdgB9PvL4j/+IVWgkwsDKnlKJeSvFDngJfy5ql2iZ\n" +
        "fiLw1wAAAXOfCo2EAAAEAwBHMEUCIGMr1sTfGhHfXG79Liqd3czev7b1bTGGa18V\n" +
        "imeXbG9jAiEA3KYA6Miao0V2wrSYHpy31hp7h1bPfms2uVE7kcgFlBMAdQDuwJXu\n" +
        "jXJkD5Ljw7kbxxKjaWoJe0tqGhQ45keyy+3F+QAAAXOfCo3/AAAEAwBGMEQCIAXH\n" +
        "rNKSTf94Y3SjIQiRzEfVPqL21+YihHThCeGagWe5AiAYC1qyRGGJFxhBaedenOG+\n" +
        "AWmXNHXZTzM5XcIGqc3/yzANBgkqhkiG9w0BAQsFAAOCAQEAKjNtCVeKDAdyRd7H\n" +
        "GL+GAuUyvaifuJsnYrZugaA4Gwv9+afZd5nkrWQbcbWiyV1GFBvtq3lQpQM21hFN\n" +
        "jEX4uww7IGpLIshGgkA5BECEWeRvJTz6Q7p7fc+lIk1PFlJW+3HQj2lAXQbqVbkv\n" +
        "/RsCVP5V9NhNZKwCSFoOrahht3KcnmxlLIBjOBQe+aF5g3MLqPuclp5lkPZUyuPE\n" +
        "n5KRzaBSxjjKY0m9MRVcl/4DhNR/R9Lc40W1EajjCNNIgQE/9Z+4J/SJsoeCj+6Q\n" +
        "e3gQiTn5iQWVaIpB6CkYs8Diw+HwW7kslmjC80oFOzJ1PiFXXlEwPDiWoiSOHoIn\n" +
        "mb0vAA==\n" +
        "-----END CERTIFICATE-----\n" +
        "-----BEGIN CERTIFICATE-----\n" +
        "MIIEtjCCA56gAwIBAgIQd70N32OE/7HRSj7dg4PIaTANBgkqhkiG9w0BAQsFADBM\n" +
        "MSAwHgYDVQQLExdHbG9iYWxTaWduIFJvb3QgQ0EgLSBSMzETMBEGA1UEChMKR2xv\n" +
        "YmFsU2lnbjETMBEGA1UEAxMKR2xvYmFsU2lnbjAeFw0yMDA3MTEwMDAwMDBaFw0y\n" +
        "OTAzMTgwMDAwMDBaMEsxCzAJBgNVBAYTAkRFMRkwFwYDVQQKExBEZXV0c2NoZSBQ\n" +
        "b3N0IEFHMSEwHwYDVQQDExhEUERITCBHbG9iYWwgVExTIENBIC0gSTUwggEiMA0G\n" +
        "CSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDq/LPe8xgyLgwimOeFnrqppaa6Srlj\n" +
        "91063O+bA0Ppi6XQFWsL+jcJlZtNXqAfuZQuvW0Qiat4lx2/4xAyQEP+Q8ekMcSb\n" +
        "Tc8ExKew/83H+mKHdOPB8J4QOhMvOPQbQa5WoI+lCLdWJcYdYnAc59TQWYZ7vMz4\n" +
        "qcjw66LXL26oalzp445gGkTw2xqAvP57FfmlCpZ9BtVpqXTgWM9eXECIcA+PHazY\n" +
        "HnbXION18s+T/rtnCtgyKrDHTr2KigsQwT9V4Seh5p1U96ZmA+68iO1YMjvddWOs\n" +
        "vbskCfWWjfx8LQ2fyr0250dotq50BUtiG04DEimwfas0otGaPMpSNdLZAgMBAAGj\n" +
        "ggGTMIIBjzAOBgNVHQ8BAf8EBAMCAYYwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsG\n" +
        "AQUFBwMCMBIGA1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0OBBYEFO9b9EuG+db5OlOJ\n" +
        "CbwgjrwayiJgMB8GA1UdIwQYMBaAFI/wS3+oLkUkrk1Q+mOai97i3Ru8MHoGCCsG\n" +
        "AQUFBwEBBG4wbDAtBggrBgEFBQcwAYYhaHR0cDovL29jc3AuZ2xvYmFsc2lnbi5j\n" +
        "b20vcm9vdHIzMDsGCCsGAQUFBzAChi9odHRwOi8vc2VjdXJlLmdsb2JhbHNpZ24u\n" +
        "Y29tL2NhY2VydC9yb290LXIzLmNydDA2BgNVHR8ELzAtMCugKaAnhiVodHRwOi8v\n" +
        "Y3JsLmdsb2JhbHNpZ24uY29tL3Jvb3QtcjMuY3JsMFYGA1UdIARPME0wCAYGZ4EM\n" +
        "AQICMEEGCSsGAQQBoDIBFDA0MDIGCCsGAQUFBwIBFiZodHRwczovL3d3dy5nbG9i\n" +
        "YWxzaWduLmNvbS9yZXBvc2l0b3J5LzANBgkqhkiG9w0BAQsFAAOCAQEAwaGX1W8P\n" +
        "hVIMRwVxEjdRip4vb5UUw9RlFZ7qej9/irzpFGdoa+6YqRpDc1kxpRxx9COzE3m3\n" +
        "QVyYw4XfHIknJCNqCfAdFzgtk1DdRRCO4PV3DGHqu8NQPWnvvx6kw8a+tJs4Eoif\n" +
        "10EN1hw4JEE+7106updRbPIJz3RErtc/3iee1BHaK4hiV9rOM5LsJ0qWRXXAVjmn\n" +
        "tnLVdTR805UFnI1KiSCURz5JTSIuYhtsdlo+iK51WXxYfaEOB0XvwIxpJ2Pq35VN\n" +
        "heh/whbOI5vUAVzXY9aVlOdXETchFy4UopYOZhKEbig1oeVXavNoqmMcRyELaPuq\n" +
        "1/x47/8wT0zxIA==\n" +
        "-----END CERTIFICATE-----\n" +
        "-----BEGIN CERTIFICATE-----\n" +
        "MIIETjCCAzagAwIBAgINAe5fFp3/lzUrZGXWajANBgkqhkiG9w0BAQsFADBXMQsw\n" +
        "CQYDVQQGEwJCRTEZMBcGA1UEChMQR2xvYmFsU2lnbiBudi1zYTEQMA4GA1UECxMH\n" +
        "Um9vdCBDQTEbMBkGA1UEAxMSR2xvYmFsU2lnbiBSb290IENBMB4XDTE4MDkxOTAw\n" +
        "MDAwMFoXDTI4MDEyODEyMDAwMFowTDEgMB4GA1UECxMXR2xvYmFsU2lnbiBSb290\n" +
        "IENBIC0gUjMxEzARBgNVBAoTCkdsb2JhbFNpZ24xEzARBgNVBAMTCkdsb2JhbFNp\n" +
        "Z24wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDMJXaQeQZ4Ihb1wIO2\n" +
        "hMoonv0FdhHFrYhy/EYCQ8eyip0EXyTLLkvhYIJG4VKrDIFHcGzdZNHr9SyjD4I9\n" +
        "DCuul9e2FIYQebs7E4B3jAjhSdJqYi8fXvqWaN+JJ5U4nwbXPsnLJlkNc96wyOkm\n" +
        "DoMVxu9bi9IEYMpJpij2aTv2y8gokeWdimFXN6x0FNx04Druci8unPvQu7/1PQDh\n" +
        "BjPogiuuU6Y6FnOM3UEOIDrAtKeh6bJPkC4yYOlXy7kEkmho5TgmYHWyn3f/kRTv\n" +
        "riBJ/K1AFUjRAjFhGV64l++td7dkmnq/X8ET75ti+w1s4FRpFqkD2m7pg5NxdsZp\n" +
        "hYIXAgMBAAGjggEiMIIBHjAOBgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/BAUwAwEB\n" +
        "/zAdBgNVHQ4EFgQUj/BLf6guRSSuTVD6Y5qL3uLdG7wwHwYDVR0jBBgwFoAUYHtm\n" +
        "GkUNl8qJUC99BM00qP/8/UswPQYIKwYBBQUHAQEEMTAvMC0GCCsGAQUFBzABhiFo\n" +
        "dHRwOi8vb2NzcC5nbG9iYWxzaWduLmNvbS9yb290cjEwMwYDVR0fBCwwKjAooCag\n" +
        "JIYiaHR0cDovL2NybC5nbG9iYWxzaWduLmNvbS9yb290LmNybDBHBgNVHSAEQDA+\n" +
        "MDwGBFUdIAAwNDAyBggrBgEFBQcCARYmaHR0cHM6Ly93d3cuZ2xvYmFsc2lnbi5j\n" +
        "b20vcmVwb3NpdG9yeS8wDQYJKoZIhvcNAQELBQADggEBACNw6c/ivvVZrpRCb8RD\n" +
        "M6rNPzq5ZBfyYgZLSPFAiAYXof6r0V88xjPy847dHx0+zBpgmYILrMf8fpqHKqV9\n" +
        "D6ZX7qw7aoXW3r1AY/itpsiIsBL89kHfDwmXHjjqU5++BfQ+6tOfUBJ2vgmLwgtI\n" +
        "fR4uUfaNU9OrH0Abio7tfftPeVZwXwzTjhuzp3ANNyuXlava4BJrHEDOxcd+7cJi\n" +
        "WOx37XMiwor1hkOIreoTbv3Y/kIvuX1erRjvlJDKPSerJpSZdcfL03v3ykzTr1Eh\n" +
        "kluEfSufFT90y1HonoMOFm8b50bOI7355KKL0jlrqnkckSziYSQtjipIcJDEHsXo\n" +
        "4HA=\n" +
        "-----END CERTIFICATE-----\n" +
        "-----BEGIN CERTIFICATE-----\n" +
        "MIIDdTCCAl2gAwIBAgILBAAAAAABFUtaw5QwDQYJKoZIhvcNAQEFBQAwVzELMAkG\n" +
        "A1UEBhMCQkUxGTAXBgNVBAoTEEdsb2JhbFNpZ24gbnYtc2ExEDAOBgNVBAsTB1Jv\n" +
        "b3QgQ0ExGzAZBgNVBAMTEkdsb2JhbFNpZ24gUm9vdCBDQTAeFw05ODA5MDExMjAw\n" +
        "MDBaFw0yODAxMjgxMjAwMDBaMFcxCzAJBgNVBAYTAkJFMRkwFwYDVQQKExBHbG9i\n" +
        "YWxTaWduIG52LXNhMRAwDgYDVQQLEwdSb290IENBMRswGQYDVQQDExJHbG9iYWxT\n" +
        "aWduIFJvb3QgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDaDuaZ\n" +
        "jc6j40+Kfvvxi4Mla+pIH/EqsLmVEQS98GPR4mdmzxzdzxtIK+6NiY6arymAZavp\n" +
        "xy0Sy6scTHAHoT0KMM0VjU/43dSMUBUc71DuxC73/OlS8pF94G3VNTCOXkNz8kHp\n" +
        "1Wrjsok6Vjk4bwY8iGlbKk3Fp1S4bInMm/k8yuX9ifUSPJJ4ltbcdG6TRGHRjcdG\n" +
        "snUOhugZitVtbNV4FpWi6cgKOOvyJBNPc1STE4U6G7weNLWLBYy5d4ux2x8gkasJ\n" +
        "U26Qzns3dLlwR5EiUWMWea6xrkEmCMgZK9FGqkjWZCrXgzT/LCrBbBlDSgeF59N8\n" +
        "9iFo7+ryUp9/k5DPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNVHRMBAf8E\n" +
        "BTADAQH/MB0GA1UdDgQWBBRge2YaRQ2XyolQL30EzTSo//z9SzANBgkqhkiG9w0B\n" +
        "AQUFAAOCAQEA1nPnfE920I2/7LqivjTFKDK1fPxsnCwrvQmeU79rXqoRSLblCKOz\n" +
        "yj1hTdNGCbM+w6DjY1Ub8rrvrTnhQ7k4o+YviiY776BQVvnGCv04zcQLcFGUl5gE\n" +
        "38NflNUVyRRBnMRddWQVDf9VMOyGj/8N7yy5Y0b2qvzfvGn9LhJIZJrglfCm7ymP\n" +
        "AbEVtQwdpf5pLGkkeB6zpxxxYu7KyJesF12KwvhHhm4qxFYxldBniYUr+WymXUad\n" +
        "DKqC5JlR3XC321Y9YeRq4VzW9v493kHMB65jUr9TU/Qr6cf9tveCX4XSQRjbgbME\n" +
        "HMUfpIBvFSDJ3gyICh3WZlXi/EjJKSZp4A==\n" +
        "-----END CERTIFICATE-----\n")
        .getBytes();

    @Test
    public void testb1() throws Exception {
        X509Certificate cert = CertUtils.cert(b1).orElse(null);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }
    @Test
    public void testc1() throws Exception {
        X509Certificate cert = CertUtils.cert(c1).orElse(null);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }
    @Test
    public void testc2() throws Exception {
        X509Certificate cert = CertUtils.cert(c2).orElse(null);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }
    @Test
    public void testc3() throws Exception {
        assertNotEquals(c3, c2);
        X509Certificate cert = CertUtils.cert(c3).orElse(null);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }

    @Test
    public void testc4() throws Exception {
        assertNotEquals(c4, c2);
        X509Certificate cert = CertUtils.cert(c4).orElse(null);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }

    @Test
    public void testc5() throws Exception {
        X509Certificate cert = CertUtils.cert(c5).orElse(null);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }

    @Test
    public void test7() throws Exception {
        X509Certificate cert = CertUtils.cert(Paths.get("chain.p7b")).orElse(null);
        PublicKey key = cert.getPublicKey();
        assertTrue(key instanceof RSAPublicKey);
        assertEquals(65537, ((RSAPublicKey)key).getPublicExponent().intValue());
        assertNotNull(CertUtils.export(cert));
    }
}