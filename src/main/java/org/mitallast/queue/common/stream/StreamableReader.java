package org.mitallast.queue.common.stream;

@FunctionalInterface
public interface StreamableReader<T extends Streamable> {

    T read(StreamInput streamInput);
}
