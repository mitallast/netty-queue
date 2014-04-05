package org.mitallast.transport;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NotFoundException extends ServiceException {

    private static final HttpResponseStatus STATUS = HttpResponseStatus.NOT_FOUND;

    public NotFoundException() {
        super(STATUS);
    }

    public NotFoundException(String message) {
        super(STATUS, message);
    }

    public NotFoundException(Throwable cause) {
        super(STATUS, cause);
    }

    public NotFoundException(String message, Throwable cause) {
        super(STATUS, message, cause);
    }
}
