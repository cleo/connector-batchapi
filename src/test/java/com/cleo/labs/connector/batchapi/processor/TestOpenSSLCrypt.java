package com.cleo.labs.connector.batchapi.processor;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestOpenSSLCrypt {

    @Test
    public void test() {
        String password = "Cleo1234";
        String source = "alphabet soup";
        String encrypted = OpenSSLCrypt.encrypt(password, source);
        assertNotEquals(encrypted, source);
        assertEquals(source, OpenSSLCrypt.decrypt(password, encrypted));
        assertEquals(encrypted, OpenSSLCrypt.decrypt("not password", encrypted));
        assertEquals(source, OpenSSLCrypt.decrypt(password, source));
    }

    @Test
    public void testlong() {
        String password = "Cleo1234";
        String source = "some longer content repeated until it is longisher";
        source += source;
        String encrypted = OpenSSLCrypt.encrypt(password, source);
        String folded = OpenSSLCrypt.fold(encrypted);
        // System.out.println(encrypted + " -->\n" + folded);
        assertNotEquals(encrypted, source);
        assertNotEquals(encrypted, folded);
        assertEquals(encrypted, OpenSSLCrypt.unfold(folded));
        assertEquals(source, OpenSSLCrypt.decrypt(password, encrypted));
        assertEquals(source, OpenSSLCrypt.decrypt(password, folded));
        assertEquals(encrypted, OpenSSLCrypt.decrypt("not password", encrypted));
        assertEquals(source, OpenSSLCrypt.decrypt(password, source));
    }
}
