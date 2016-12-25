package org.mitallast.queue.benchmark;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class BenchmarkResponse implements Streamable {
    private final long request;

    public BenchmarkResponse(long request) {
        this.request = request;
    }

    public BenchmarkResponse(StreamInput stream) throws IOException {
        this.request = stream.readLong();
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(request);
    }

    public long getRequest() {
        return request;
    }
}
