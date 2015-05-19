package org.mitallast.queue.common.stream;

import java.io.IOException;

public interface Streamable {

    void readFrom(StreamInput stream) throws IOException;

    void writeTo(StreamOutput stream) throws IOException;
}
