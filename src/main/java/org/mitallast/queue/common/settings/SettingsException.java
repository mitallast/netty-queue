package org.mitallast.queue.common.settings;

import org.mitallast.queue.QueueConfigurationException;

public class SettingsException extends QueueConfigurationException {

    public SettingsException(String message) {
        super(message);
    }

    public SettingsException(String message, Throwable cause) {
        super(message, cause);
    }
}
