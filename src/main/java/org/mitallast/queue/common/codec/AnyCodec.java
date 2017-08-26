package org.mitallast.queue.common.codec;

import com.google.common.base.Preconditions;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

@SuppressWarnings("unchecked")
final class AnyCodec<T extends Message> implements Codec<T> {
    private final static TIntObjectMap<Codec> idToCodecMap = new TIntObjectHashMap<>(100, 0.5f, -1);
    private final static TObjectIntMap<Class> classToIdMap = new TObjectIntHashMap<>(100, 0.5f, -1);

    public static synchronized <T extends Message> void register(int code, Class<T> type, Codec<T> codec) {
        Codec c = idToCodecMap.putIfAbsent(code, codec);
        Preconditions.checkArgument(c == null, "code already registered: " + code);
        int i = classToIdMap.putIfAbsent(type, code);
        Preconditions.checkArgument(i < 0, "class already registered: " + type);
    }

    @Override
    public T read(DataInput stream) {
        try {
            int id = stream.readInt();
            Preconditions.checkArgument(id >= 0);
            Codec codec = idToCodecMap.get(id);
            Preconditions.checkNotNull(codec);
            return (T) codec.read(stream);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void write(DataOutput stream, T value) {
        try {
            int id = classToIdMap.get(value.getClass());
            Preconditions.checkArgument(id >= 0);
            Codec codec = idToCodecMap.get(id);
            stream.writeInt(id);
            codec.write(stream, value);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
