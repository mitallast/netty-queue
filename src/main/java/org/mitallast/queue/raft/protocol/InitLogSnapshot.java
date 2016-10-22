package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class InitLogSnapshot implements Streamable {

    public static final InitLogSnapshot INSTANCE = new InitLogSnapshot();

    public static InitLogSnapshot read(StreamInput stream) throws IOException {
        return INSTANCE;
    }

    private InitLogSnapshot() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
