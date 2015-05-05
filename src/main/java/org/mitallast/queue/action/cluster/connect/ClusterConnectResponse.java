package org.mitallast.queue.action.cluster.connect;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class ClusterConnectResponse extends ActionResponse {

    private boolean connected;

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        connected = stream.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeBoolean(connected);
    }
}
