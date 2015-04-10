package org.mitallast.queue.transport.client;

import org.mitallast.queue.common.concurrent.futures.Mapper;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.transport.TransportFrame;

import java.util.function.Supplier;

public class ResponseMapper<T extends Streamable> implements Mapper<TransportFrame, T> {

    private final Supplier<T> factory;

    public ResponseMapper(Supplier<T> factory) {
        this.factory = factory;
    }

    @Override
    public T map(TransportFrame value) throws Exception {
        try (StreamInput streamInput = value.inputStream()) {
            T streamable = factory.get();
            streamable.readFrom(streamInput);
            return streamable;
        }
    }
}
