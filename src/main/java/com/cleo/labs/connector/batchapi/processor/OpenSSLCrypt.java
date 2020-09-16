package com.cleo.labs.connector.batchapi.processor;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.base.Charsets;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Mimics the OpenSSL AES Cipher options for encrypting and decrypting messages
 * using a shared key (aka password) with symmetric ciphers.
 * 
 * Adapted from https://stackoverflow.com/a/41495143
 */
public class OpenSSLCrypt {

    /** OpenSSL's magic initial bytes. */
    private static final String SALTED_STR = "Salted__";
    private static final byte[] SALTED_MAGIC = SALTED_STR.getBytes(Charsets.US_ASCII);

    public static String encryptAndURLEncode(String password, String clearText) {
        String encrypted = encrypt(password, clearText);
        try {
            return URLEncoder.encode(encrypted, Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException impossible) {
            throw new RuntimeException(impossible);
        }
    }

    /**
     *
     * @param password The password / key to encrypt with.
     * @param data     The data to encrypt
     * @return A base64 encoded string containing the encrypted data.
     */
    public static String encrypt(String password, String clearText) {
        try {
            final byte[] pass = password.getBytes(Charsets.US_ASCII);
            final byte[] salt = (new SecureRandom()).generateSeed(8);
            final byte[] inBytes = clearText.getBytes(Charsets.UTF_8);

            final byte[] passAndSalt = ArrayUtils.addAll(pass, salt);
            byte[] hash = new byte[0];
            byte[] keyAndIv = new byte[0];
            for (int i = 0; i < 3 && keyAndIv.length < 48; i++) {
                final byte[] hashData = ArrayUtils.addAll(hash, passAndSalt);
                final MessageDigest md = MessageDigest.getInstance("MD5");
                hash = md.digest(hashData);
                keyAndIv = ArrayUtils.addAll(keyAndIv, hash);
            }

            final byte[] keyValue = Arrays.copyOfRange(keyAndIv, 0, 32);
            final byte[] iv = Arrays.copyOfRange(keyAndIv, 32, 48);
            final SecretKeySpec key = new SecretKeySpec(keyValue, "AES");

            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
            byte[] data = cipher.doFinal(inBytes);
            data = ArrayUtils.addAll(ArrayUtils.addAll(SALTED_MAGIC, salt), data);
            return Base64.getEncoder().encodeToString(data);
        } catch (Exception impossible) {
            throw new RuntimeException(impossible);
        }
    }

    /**
     * @see http://stackoverflow.com/questions/32508961/java-equivalent-of-an-openssl-aes-cbc-encryption
     *      for what looks like a useful answer. The not-yet-commons-ssl also has an
     *      implementation
     * @param password
     * @param source   The encrypted data
     * @return
     */
    public static String decrypt(String password, String source) {
        try {
            final byte[] pass = password.getBytes(Charsets.US_ASCII);

            final byte[] inBytes = Base64.getDecoder().decode(source);

            final byte[] shouldBeMagic = Arrays.copyOfRange(inBytes, 0, SALTED_MAGIC.length);
            if (!Arrays.equals(shouldBeMagic, SALTED_MAGIC)) {
                throw new IllegalArgumentException(
                        "Initial bytes from input do not match OpenSSL SALTED_MAGIC salt value.");
            }

            final byte[] salt = Arrays.copyOfRange(inBytes, SALTED_MAGIC.length, SALTED_MAGIC.length + 8);

            final byte[] passAndSalt = ArrayUtils.addAll(pass, salt);

            byte[] hash = new byte[0];
            byte[] keyAndIv = new byte[0];
            for (int i = 0; i < 3 && keyAndIv.length < 48; i++) {
                final byte[] hashData = ArrayUtils.addAll(hash, passAndSalt);
                final MessageDigest md = MessageDigest.getInstance("MD5");
                hash = md.digest(hashData);
                keyAndIv = ArrayUtils.addAll(keyAndIv, hash);
            }

            final byte[] keyValue = Arrays.copyOfRange(keyAndIv, 0, 32);
            final SecretKeySpec key = new SecretKeySpec(keyValue, "AES");

            final byte[] iv = Arrays.copyOfRange(keyAndIv, 32, 48);

            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
            final byte[] clear = cipher.doFinal(inBytes, 16, inBytes.length - 16);
            return new String(clear, Charsets.UTF_8);
        } catch (Exception impossible) {
            throw new RuntimeException(impossible);
        }
    }
}
