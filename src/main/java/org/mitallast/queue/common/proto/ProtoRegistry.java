package org.mitallast.queue.common.proto;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Parser;

public class ProtoRegistry {
    private final int index;
    private final Descriptor descriptor;
    private final Parser parser;


    public ProtoRegistry(int index, Descriptor descriptor, Parser parser) {
        this.index = index;
        this.descriptor = descriptor;
        this.parser = parser;
    }

    public int getIndex() {
        return index;
    }

    public Descriptor getDescriptor() {
        return descriptor;
    }

    public Parser getParser() {
        return parser;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProtoRegistry that = (ProtoRegistry) o;

        return index == that.index;
    }

    @Override
    public int hashCode() {
        return index;
    }
}
