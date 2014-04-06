package org.mitallast.queue;

public class QueueConfigurationException extends QueueException {

    public QueueConfigurationException(String msg) {
        super(msg);
    }

    public QueueConfigurationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}