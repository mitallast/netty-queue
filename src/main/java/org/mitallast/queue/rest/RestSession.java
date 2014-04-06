package org.mitallast.queue.rest;

public interface RestSession {

    void sendResponse(RestResponse response);

    void sendResponse(Throwable response);
}
