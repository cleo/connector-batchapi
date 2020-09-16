package com.cleo.labs.connector.batchapi.processor;

public class ProcessingException extends Exception {
    /**
     * Exception processing an entry
     */
    private static final long serialVersionUID = 8577432753434980463L;

    public ProcessingException(String message) {
        super(message);
    }
}
