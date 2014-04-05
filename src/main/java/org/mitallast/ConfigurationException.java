package org.mitallast;

public class ConfigurationException extends RuntimeException {
    private static final long serialVersionUID = -4891898485346985591L;

    public ConfigurationException() {
    }

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}

