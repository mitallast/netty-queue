package org.mitallast.queue.common.stream;

import java.io.IOException;

public interface Streamable {

    public void readFrom(StreamInput stream) throws IOException;

    public void writeTo(StreamOutput stream) throws IOException;
}
