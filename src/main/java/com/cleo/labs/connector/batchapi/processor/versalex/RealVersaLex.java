package com.cleo.labs.connector.batchapi.processor.versalex;

import java.io.File;

import com.cleo.lexicom.beans.LexBean;
//import com.cleo.lexicom.LexiCom;
import com.cleo.lexicom.external.ILexiCom;
import com.cleo.lexicom.external.LexiComFactory;
import com.cleo.security.encryption.ConfigEncryption;
import com.google.gwt.thirdparty.guava.common.base.Strings;

public class RealVersaLex implements VersaLex {

    private ILexiCom ilexicom;
    private boolean vended = false;

    @Override
    public void connect() {
        try {
            ilexicom = LexiComFactory.getCurrentInstance();
        } catch (Exception e) {
            // try again to connect and get an instance
            try {
                String home = System.getenv("CLEOHOME");
                if (Strings.isNullOrEmpty(home)) {
                    home = ".";
                }
                int product = new File(home, "Harmonyc").exists() ? LexiComFactory.HARMONY : LexiComFactory.VLTRADER;
                ilexicom = LexiComFactory.getVersaLex(product,
                        new File(home).getAbsolutePath(),
                        LexiComFactory.CLIENT_ONLY);
                vended = true;
            } catch (Exception f) {
                // now give up officially
                f.printStackTrace();
                ilexicom = null;
            }
        }
    }

    @Override
    public void disconnect() {
        if (ilexicom != null && vended) {
            try {
                ilexicom.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            ilexicom = null;
            vended = false;
        }

    }

    @Override
    public String get(String host, String mailbox, String property) throws Exception {
        try {
            String result = get(ILexiCom.MAILBOX, new String[] {host, mailbox}, property);
            return result;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String get(String host, String property) throws Exception {
        try {
            return get(ILexiCom.HOST, new String[] {host}, property);
        } catch (Exception e) {
            try {
                return get(ILexiCom.MAILBOX, new String[] {host, host}, property);
            } catch (Exception f) {
                return null; // usually it just means "not found"
            }
        }
    }

    private String get(int type, String[] path, String property) throws Exception {
        String[] result = ilexicom.getProperty(type, path, property);
        return result == null ? null : result[0];
    }

    @Override
    public void set(String host, String mailbox, String property, String value) throws Exception {
        set(ILexiCom.MAILBOX, new String[] {host, mailbox}, property, value);
    }

    @Override
    public void set(String host, String property, String value) throws Exception {
        try {
            set(ILexiCom.HOST, new String[] {host}, property, value);
        } catch (Exception e) {
            set(ILexiCom.MAILBOX, new String[] {host, host}, property, value);
        }
    }

    private void set(int type, String[] path, String property, String value) throws Exception {
        ilexicom.setProperty(type, path, property, value);
    }

    @Override
    public String decrypt(String s) {
        if (s != null && (s.startsWith("vlenc:") || s.startsWith("*"))) {
            s = LexBean.vlDecrypt((ConfigEncryption) null, s);
        }
        return s;
    }

    @Override
    public boolean connected() {
        return ilexicom != null;
    }
}