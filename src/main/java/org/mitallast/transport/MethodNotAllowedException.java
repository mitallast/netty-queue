package org.mitallast.transport;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collections;
import java.util.List;

public class MethodNotAllowedException extends ServiceException {

    private static final HttpResponseStatus STATUS = HttpResponseStatus.METHOD_NOT_ALLOWED;

    private List<HttpMethod> allowedMethods;

    public MethodNotAllowedException(List<HttpMethod> allowed) {
        super(STATUS);
        setAllowedMethods(allowed);
    }

    public MethodNotAllowedException(String message, List<HttpMethod> allowed) {
        super(STATUS, message);
        setAllowedMethods(allowed);
    }

    public MethodNotAllowedException(Throwable cause, List<HttpMethod> allowed) {
        super(STATUS, cause);
        setAllowedMethods(allowed);
    }

    public MethodNotAllowedException(String message, Throwable cause, List<HttpMethod> allowed) {
        super(STATUS, message, cause);
        setAllowedMethods(allowed);
    }

    public List<HttpMethod> getAllowedMethods() {
        return allowedMethods;
    }

    public void setAllowedMethods(List<HttpMethod> allowed) {
        this.allowedMethods = Collections.unmodifiableList(allowed);
    }
}
