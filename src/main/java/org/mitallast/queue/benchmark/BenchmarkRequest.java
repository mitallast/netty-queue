package org.mitallast.queue.benchmark;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class BenchmarkRequest implements Streamable {
    private final long request;
    private final ByteBuf data;

    public BenchmarkRequest(long request, ByteBuf data) {
        this.request = request;
        this.data = data;
    }

    public BenchmarkRequest(StreamInput stream) throws IOException {
        this.request = stream.readLong();
        this.data = stream.readByteBuf();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(request);
        stream.writeByteBuf(data);
    }

    public long getRequest() {
        return request;
    }

    public ByteBuf getData() {
        return data;
    }
}
