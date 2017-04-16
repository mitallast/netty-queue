package org.mitallast.queue.common.stream;

public interface StreamableClassRegistry {

    <T extends Streamable> void writeClass(StreamOutput stream, Class<T> streamableClass);

    <T extends Streamable> T readStreamable(StreamInput stream);
}
