package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class JointTimeout implements Streamable {

    public static final JointTimeout INSTANCE = new JointTimeout();

    public static JointTimeout read(StreamInput stream) throws IOException {
        return INSTANCE;
    }

    private JointTimeout() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
