package org.mitallast.transport;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

public class Response {
    private HttpResponseStatus responseStatus = HttpResponseStatus.OK;
    private HttpHeaders headers = new DefaultHttpHeaders();
    private Object body;
    private Throwable exception = null;

    public Response() {
    }

    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
    }

    public boolean hasBody() {
        return (getBody() != null);
    }


    public HttpHeaders getHeaders(){return headers; }

    public void addLocationHeader(String url) {
        headers.set(HttpHeaders.Names.LOCATION, url);
    }

    public void setResponseStatus(int value) {
        setResponseStatus(HttpResponseStatus.valueOf(value));
    }

    public void setResponseCreated() {
        setResponseStatus(HttpResponseStatus.CREATED);
    }

    public void setResponseNoContent() {
        setResponseStatus(HttpResponseStatus.NO_CONTENT);
    }

    public HttpResponseStatus getResponseStatus() {
        return responseStatus;
    }

    public void setResponseStatus(HttpResponseStatus status) {
        this.responseStatus = status;
    }

    public CharSequence getContentType() {
        return headers.get(HttpHeaders.Names.CONTENT_TYPE);
    }

    public void setContentType(String contentType) {
        headers.set(HttpHeaders.Names.CONTENT_TYPE, contentType);
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
        if(exception instanceof ServiceException){
            this.responseStatus = ((ServiceException)exception).getHttpStatus();
            this.headers.set(headers);
        }
    }

    public boolean hasException() {
        return exception != null;
    }
}
