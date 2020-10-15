package com.cleo.labs.connector.batchapi.processor;

public class UnexpectedCodeException extends ProcessingException {
    /**
     * Exception with an HTTP status code
     */
    private static final long serialVersionUID = 459137439406273436L;

    private int code;

    public UnexpectedCodeException(String message, int code) {
        super(message);
        this.code = code;
    }

    public int code() {
        return this.code;
    }

}
