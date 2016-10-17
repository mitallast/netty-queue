package org.mitallast.queue.raft2.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class BeginElection implements ElectionMessage {

    public final static BeginElection INSTANCE = new BeginElection();

    protected BeginElection() {
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {

    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {

    }
}
