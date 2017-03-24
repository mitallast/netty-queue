package org.mitallast.queue.common.stream;

@Deprecated
public class StreamableRegistry<T extends Streamable> {
    private final int id;
    private final Class<T> streamable;
    private final StreamableReader<T> reader;

    public static <T extends Streamable> StreamableRegistry of(Class<T> streamable, StreamableReader<T> reader, int id) {
        return new StreamableRegistry<T>(id, streamable, reader);
    }

    private StreamableRegistry(int id, Class<T> streamable, StreamableReader<T> reader) {
        this.id = id;
        this.streamable = streamable;
        this.reader = reader;
    }

    public int getId() {
        return id;
    }

    public Class<? extends Streamable> getStreamable() {
        return streamable;
    }

    public StreamableReader<T> getReader() {
        return reader;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamableRegistry that = (StreamableRegistry) o;

        return id == that.id;

    }

    @Override
    public int hashCode() {
        return id;
    }
}
