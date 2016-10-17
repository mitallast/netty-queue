package org.mitallast.queue.common.stream;

import java.io.IOException;

public interface Streamable {

    void writeTo(StreamOutput stream) throws IOException;
}
