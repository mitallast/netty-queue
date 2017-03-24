package org.mitallast.queue.common.proto;

import com.google.inject.Inject;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.mitallast.queue.proto.raft.Any;

import java.io.IOException;
import java.util.Set;

public class ProtoService {
    private static final float LOAD_FACTOR = 0.5f;
    private static final int NO_ENTRY_KEY = -1;

    private final TIntObjectMap<Descriptor> indexDescriptors;
    private final TIntObjectMap<Parser> indexParsers;
    private final TObjectIntMap<Descriptor> descriptorIndexes;

    @Inject
    public ProtoService(Set<ProtoRegistry> registries) {
        indexDescriptors = new TIntObjectHashMap<>(registries.size(), LOAD_FACTOR, NO_ENTRY_KEY);
        indexParsers = new TIntObjectHashMap<>(registries.size(), LOAD_FACTOR, NO_ENTRY_KEY);
        descriptorIndexes = new TObjectIntHashMap<>(registries.size(), LOAD_FACTOR, NO_ENTRY_KEY);

        for (ProtoRegistry registry : registries) {
            indexDescriptors.put(registry.getIndex(), registry.getDescriptor());
            descriptorIndexes.put(registry.getDescriptor(), registry.getIndex());
            indexParsers.put(registry.getIndex(), registry.getParser());
        }
    }

    public int index(Descriptor descriptor) {
        int index = descriptorIndexes.get(descriptor);
        if (index == NO_ENTRY_KEY) {
            throw new IllegalArgumentException("Descriptor not registered: " + descriptor.getFullName());
        }
        return index;
    }

    public Descriptor descriptor(int index) {
        Descriptor descriptor = indexDescriptors.get(index);
        if (descriptor == null) {
            throw new IllegalArgumentException("index not registered: " + index);
        }
        return descriptor;
    }

    public Parser parser(int index) {
        Parser parser = indexParsers.get(index);
        if (parser == null) {
            throw new IllegalArgumentException("index not registered: " + index);
        }
        return parser;
    }

    public Any pack(Message message) {
        int index = index(message.getDescriptorForType());
        return Any.newBuilder()
            .setIndex(index)
            .setValue(message.toByteString())
            .build();
    }

    @SuppressWarnings({"unchecked", "unused"})
    public <T extends com.google.protobuf.Message> T unpack(Any any, Parser<T> parser) {
        try {
            return parser.parseFrom(any.getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public Message unpack(Any any) {
        try {
            int index = any.getIndex();
            Object o = parser(index).parseFrom(any.getValue());
            return (Message) o;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean is(Any any, Descriptor descriptor) {
        int index = any.getIndex();
        return descriptor(index).equals(descriptor);
    }

    public <T extends Message> T readDelimited(CodedInputStream coded, Parser<T> parser) throws IOException {
        int size = coded.readRawVarint32();
        int limit = coded.pushLimit(size);
        try {
            T message = parser.parseFrom(coded, ExtensionRegistryLite.getEmptyRegistry());
            try {
                coded.checkLastTagWas(0);
            } catch (InvalidProtocolBufferException e) {
                throw e.setUnfinishedMessage(message);
            }
            return message;
        } finally {
            coded.popLimit(limit);
        }
    }

    public <T extends Message> void writeDelimited(CodedOutputStream coded, T message) throws IOException {
        int serialized = message.getSerializedSize();
        coded.writeUInt32NoTag(serialized);
        message.writeTo(coded);
    }
}
