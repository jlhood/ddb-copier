package com.jlhood.ddbcopier.dagger;

/**
 * Helper class for fetching environment values.
 */
public final class Env {
    public static final String DESTINATION_TABLE_KEY = "DESTINATION_TABLE_NAME";

    private Env() {
    }

    public static String getDestinationTable() {
        return System.getenv(DESTINATION_TABLE_KEY);
    }
}
