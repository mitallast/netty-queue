package org.mitallast.queue.common.stream;

import com.google.inject.Inject;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.*;

public class InternalStreamService extends AbstractComponent implements StreamableClassRegistry, StreamService {

    private final TIntObjectMap<Class<? extends Streamable>> idToClassMap = new TIntObjectHashMap<>(100, 0.5f, -1);
    private final TObjectIntMap<Class<? extends Streamable>> classToIdMap = new TObjectIntHashMap<>(100, 0.5f, -1);

    @Inject
    public InternalStreamService(Settings settings) {
        super(settings);
    }

    @Override
    public synchronized void registerClass(Class<? extends Streamable> streamableClass, int id) {
        Class<? extends Streamable> current = idToClassMap.putIfAbsent(id, streamableClass);
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
    public <T extends Streamable> Class<T> readClass(StreamInput stream) throws IOException {
        int id = stream.readInt();
        Class streamableClass = idToClassMap.get(id);
        if (streamableClass == null) {
            throw new IOException("Class id not registered: " + id);
        }
        return streamableClass;
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
        return input((DataInput) new DataInputStream(inputStream));
    }

    @Override
    public StreamInput input(DataInput dataInput) throws IOException {
        return new DataStreamInput(this, dataInput);
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
    public StreamOutput output(OutputStream outputStream) throws IOException {
        return output((DataOutput) new DataOutputStream(outputStream));
    }

    @Override
    public StreamOutput output(DataOutput dataOutput) throws IOException {
        return new DataStreamOutput(this, dataOutput);
    }
}
