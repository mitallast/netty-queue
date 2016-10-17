package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class ElectionTimeout implements ElectionMessage {

    public final static ElectionTimeout INSTANCE = new ElectionTimeout();

    public static ElectionTimeout read(StreamInput input) {
        return INSTANCE;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
