package org.mitallast.queue.common.stream;

import com.google.inject.Inject;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.netty.buffer.ByteBuf;

import java.io.*;
import java.util.Set;

public class InternalStreamService implements StreamableClassRegistry, StreamService {
    private final TIntObjectMap<StreamableReader<? extends Streamable>> idToReaderMap = new TIntObjectHashMap<>(100, 0.5f, -1);
    private final TObjectIntMap<Class<? extends Streamable>> classToIdMap = new TObjectIntHashMap<>(100, 0.5f, -1);

    @SuppressWarnings("unchecked")
    @Inject
    public InternalStreamService(Set<StreamableRegistry> registrySet) {
        for (StreamableRegistry streamableRegistry : registrySet) {
            register(streamableRegistry.getStreamable(), streamableRegistry.getReader(), streamableRegistry.getId());
        }
    }

    private <T extends Streamable> void register(Class<T> streamableClass, StreamableReader<T> reader, int id) {
        StreamableReader<? extends Streamable> current = idToReaderMap.putIfAbsent(id, reader);
        if (current != null) {
            throw new IllegalArgumentException("Class id already registered, class: " + streamableClass + " id: " + id);
        }
        classToIdMap.put(streamableClass, id);
    }

    @Override
    public <T extends Streamable> void writeClass(StreamOutput stream, Class<T> streamableClass) throws IOException {
        int id = classToIdMap.get(streamableClass);
        if (id < 0) {
            throw new IOException("Class not registered: " + streamableClass);
        }
        stream.writeInt(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Streamable> T readStreamable(StreamInput stream) throws IOException {
        int id = stream.readInt();
        StreamableReader<T> reader = (StreamableReader<T>) idToReaderMap.get(id);
        if (reader == null) {
            throw new IllegalArgumentException("Streamable " + id + " not registered");
        }
        return reader.read(stream);
    }

    @Override
    public StreamInput input(ByteBuf buffer) {
        return new ByteBufStreamInput(this, buffer);
    }

    @Override
    public StreamInput input(File file) throws IOException {
        return new DataStreamInput(this, new BufferedInputStream(new FileInputStream(file), 65536));
    }

    @Override
    public StreamOutput output(ByteBuf buffer) {
        return new ByteBufStreamOutput(this, buffer);
    }

    @Override
    public StreamOutput output(File file) throws IOException {
        return output(file, false);
    }

    @Override
    public StreamOutput output(File file, boolean append) throws IOException {
        return new DataStreamOutput(this, new BufferedOutputStream(new FileOutputStream(file, append), 65536));
    }
}
