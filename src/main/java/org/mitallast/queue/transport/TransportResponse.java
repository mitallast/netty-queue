package org.mitallast.queue.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.rest.RestResponse;

import java.io.OutputStream;

public class TransportResponse implements RestResponse {
    private HttpResponseStatus responseStatus = HttpResponseStatus.OK;
    private HttpHeaders headers = new DefaultHttpHeaders(false);
    private ByteBuf buffer = null;
    private Throwable exception = null;

    public TransportResponse() {
    }

    public boolean hasBuffer() {
        return buffer != null;
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    public OutputStream getOutputStream() {
        if (buffer == null) {
            buffer = Unpooled.buffer(0);
        }
        return new ByteBufOutputStream(buffer);
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

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

    public void setContentType(CharSequence contentType) {
        headers.set(HttpHeaders.Names.CONTENT_TYPE, contentType);
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }

    public boolean hasException() {
        return exception != null;
    }
}
