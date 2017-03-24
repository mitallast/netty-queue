package org.mitallast.queue.common.stream;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.component.AbstractComponent;

import java.io.*;
import java.util.Set;

@Deprecated
public class InternalStreamService extends AbstractComponent implements StreamableClassRegistry, StreamService {
    private final TIntObjectMap<StreamableReader<? extends Streamable>> idToReaderMap = new TIntObjectHashMap<>(100, 0.5f, -1);
    private final TObjectIntMap<Class<? extends Streamable>> classToIdMap = new TObjectIntHashMap<>(100, 0.5f, -1);

    @SuppressWarnings("unchecked")
    @Inject
    public InternalStreamService(Config config, Set<StreamableRegistry> registrySet) {
        super(config, StreamService.class);

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
        return (T) idToReaderMap.get(id).read(stream);
    }

    @Override
    public StreamInput input(ByteBuf buffer) {
        return new ByteBufStreamInput(this, buffer);
    }

    @Override
    public StreamInput input(ByteBuf buffer, int size) {
        return new ByteBufStreamInput(this, buffer, size);
    }

    @Override
    public StreamInput input(File file) throws IOException {
        return input(new FileInputStream(file));
    }

    @Override
    public StreamInput input(InputStream inputStream) throws IOException {
        return new DataStreamInput(this, inputStream);
    }

    @Override
    public StreamOutput output(ByteBuf buffer) {
        return new ByteBufStreamOutput(this, buffer);
    }

    @Override
    public StreamOutput output(File file) throws IOException {
        return output(new FileOutputStream(file));
    }

    @Override
    public StreamOutput output(File file, boolean append) throws IOException {
        return output(new FileOutputStream(file, append));
    }

    @Override
    public StreamOutput output(OutputStream outputStream) throws IOException {
        return output(new DataOutputStream(outputStream));
    }

    @Override
    public StreamOutput output(DataOutputStream dataOutput) throws IOException {
        return new DataStreamOutput(this, dataOutput);
    }
}
