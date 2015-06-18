package org.mitallast.queue.common.stream;

import java.io.IOException;

public interface StreamableClassRegistry {

    <T extends Streamable> void writeClass(StreamOutput stream, Class<T> streamableClass) throws IOException;

    <T extends Streamable> Class<T> readClass(StreamInput stream) throws IOException;
}
