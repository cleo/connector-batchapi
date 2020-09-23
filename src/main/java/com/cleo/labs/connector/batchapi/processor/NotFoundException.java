package com.cleo.labs.connector.batchapi.processor;

public class NotFoundException extends ProcessingException {
    private static final long serialVersionUID = 6820801073588708267L;

    /**
     * Exception processing an entry
     */

    public NotFoundException(String message) {
        super(message);
    }
}
