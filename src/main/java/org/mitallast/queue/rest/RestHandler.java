package org.mitallast.queue.rest;

public interface RestHandler {
    void handleRequest(RestRequest request, RestSession session);
}