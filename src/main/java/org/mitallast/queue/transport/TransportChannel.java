package org.mitallast.queue.transport;

import com.google.protobuf.Message;

import java.io.IOException;

public interface TransportChannel {

    void send(Message message) throws IOException;

    void close();
}
