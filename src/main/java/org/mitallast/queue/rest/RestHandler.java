package org.mitallast.queue.rest;

import java.io.IOException;

public interface RestHandler {
    void handleRequest(RestRequest request) throws IOException;
}