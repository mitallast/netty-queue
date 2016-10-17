package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class BeginElection implements ElectionMessage {

    public final static BeginElection INSTANCE = new BeginElection();

    public static BeginElection read(StreamInput streamInput) throws IOException {
        return INSTANCE;
    }

    private BeginElection() {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
