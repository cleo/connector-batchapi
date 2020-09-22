package com.cleo.labs.connector.batchapi.processor.versalex;

import com.cleo.lexicom.beans.LexBean;
//import com.cleo.lexicom.LexiCom;
import com.cleo.lexicom.external.ILexiCom;
import com.cleo.lexicom.external.LexiComFactory;
import com.cleo.security.encryption.ConfigEncryption;

public class RealVersaLex implements VersaLex {

//  private static LexiCom lexicom;
    private static ILexiCom ilexicom;

    static {
        try {
        //  lexicom = LexiCom.getCurrentInstance();
            ilexicom = LexiComFactory.getCurrentInstance();
        } catch (Exception e) {
            e.printStackTrace();
        //  lexicom = null;
            ilexicom = null;
        }
    }

    @Override
    public String get(String host, String mailbox, String property) throws Exception {
        try {
            return get(ILexiCom.MAILBOX, new String[] {host, mailbox}, property);
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
        return ilexicom.getProperty(type, path, property)[0];
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