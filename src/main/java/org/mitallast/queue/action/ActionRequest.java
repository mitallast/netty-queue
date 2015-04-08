package org.mitallast.queue.action;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public abstract class ActionRequest implements Streamable {
    public abstract ActionRequestValidationException validate();

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        throw new UnsupportedOperationException();
    }
}
