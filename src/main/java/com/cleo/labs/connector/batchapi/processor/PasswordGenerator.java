package com.cleo.labs.connector.batchapi.processor;

import java.security.SecureRandom;

public class PasswordGenerator {
    private static final String UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String LOWER = "abcdefghijklmnopqrstuvwxyz";
    private static final String DIGIT = "0123456789";
    private static final String SEPARATOR = "-=:|+_/.";

    private static class RandomBuilder {
        private SecureRandom random = new SecureRandom();
        private StringBuilder s = new StringBuilder();

        public RandomBuilder add(String set, int n) {
            random.ints(0, set.length()).limit(n).mapToObj(set::charAt).forEach(s::append);
            return this;
        }

        @Override
        public String toString() {
            return s.toString();
        }
    }

    public static String generatePassword() {
        // formula: 5xupper sep 5xdigit sep 5xlower sep 5xdigit
        return new RandomBuilder()
                .add(UPPER, 5)
                .add(SEPARATOR, 1)
                .add(DIGIT, 5)
                .add(SEPARATOR, 1)
                .add(LOWER, 5)
                .add(SEPARATOR, 1)
                .add(DIGIT, 5)
                .toString();
    }

}
