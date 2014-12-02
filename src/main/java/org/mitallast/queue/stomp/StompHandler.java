package org.mitallast.queue.stomp;

import io.netty.handler.codec.stomp.StompFrame;
import org.mitallast.queue.stomp.transport.StompSession;

public interface StompHandler {
    void handleRequest(StompSession session, StompFrame request);
}
