package com.cleo.labs.connector.batchapi.processor;

public class ProcessingException extends Exception {
    /**
     * Exception processing an entry
     */
    private static final long serialVersionUID = 3819802216328740863L;

    public ProcessingException(String message) {
        super(message);
    }

    public ProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
