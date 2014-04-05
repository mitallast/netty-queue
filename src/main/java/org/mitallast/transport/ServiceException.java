package org.mitallast.transport;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.Map;

public class ServiceException extends RuntimeException {

    private static final HttpResponseStatus STATUS = HttpResponseStatus.INTERNAL_SERVER_ERROR;
    private HttpResponseStatus httpStatus;
    private Map<String, String> headers;


    public ServiceException() {
        this((String) null);
    }

    public ServiceException(HttpResponseStatus status) {
        this(status, (String) null);
    }

    public ServiceException(String message) {
        this(STATUS, message);
    }

    public ServiceException(HttpResponseStatus status, String message) {
        super(message);
        httpStatus = status;
    }

    public ServiceException(Throwable cause) {
        this(STATUS, cause);
    }

    public ServiceException(HttpResponseStatus status, Throwable cause) {
        super(cause);
        httpStatus = status;
    }

    public ServiceException(String message, Throwable cause) {
        this(STATUS, message, cause);
    }

    public ServiceException(HttpResponseStatus status, String message, Throwable cause) {
        super(message, cause);
        httpStatus = status;
    }

    public static boolean isAssignableFrom(Throwable exception) {
        return ServiceException.class.isAssignableFrom(exception.getClass());
    }

    public HttpResponseStatus getHttpStatus() {
        return httpStatus;
    }

    public void setHeader(String name, String value) {
        if (headers == null) {
            headers = new HashMap<String, String>();
        }

        headers.put(name, value);
    }

    public boolean hasHeaders() {
        return (headers != null && !headers.isEmpty());
    }

    public String getHeader(String name) {
        return (headers == null ? null : headers.get(name));
    }

    public Map<String, String> headers(){
        return headers;
    }
}
