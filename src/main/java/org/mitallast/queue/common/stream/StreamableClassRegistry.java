package org.mitallast.queue.common.stream;

import java.io.IOException;

@Deprecated
public interface StreamableClassRegistry {

    <T extends Streamable> void writeClass(StreamOutput stream, Class<T> streamableClass) throws IOException;

    <T extends Streamable> T readStreamable(StreamInput stream) throws IOException;
}
