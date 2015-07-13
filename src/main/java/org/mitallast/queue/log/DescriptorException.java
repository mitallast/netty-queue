package org.mitallast.queue.log;

public class DescriptorException extends LogException {

    public DescriptorException(String message, Object... args) {
        super(String.format(message, args));
    }

    public DescriptorException(Throwable cause, String message, Object... args) {
        super(String.format(message, args), cause);
    }

    public DescriptorException(Throwable cause) {
        super(cause);
    }

}
