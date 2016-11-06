package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class ApplyCommittedLog implements Streamable {
    public static final ApplyCommittedLog INSTANCE = new ApplyCommittedLog();

    public static ApplyCommittedLog read(StreamInput stream) throws IOException {
        return INSTANCE;
    }

    private ApplyCommittedLog() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
