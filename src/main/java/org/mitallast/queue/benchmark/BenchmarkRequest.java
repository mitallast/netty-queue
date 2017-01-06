package org.mitallast.queue.benchmark;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class BenchmarkRequest implements Streamable {
    private final long request;
    private final byte[] data;

    public BenchmarkRequest(long request, byte[] data) {
        this.request = request;
        this.data = data;
    }

    public BenchmarkRequest(StreamInput stream) throws IOException {
        this.request = stream.readLong();
        int size = stream.readInt();
        if (size > 0) {
            this.data = new byte[size];
            stream.readFully(this.data);
        } else {
            this.data = new byte[0];
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(request);
        stream.writeInt(data.length);
        if (data.length > 0) {
            stream.write(data);
        }
    }

    public long getRequest() {
        return request;
    }

    public byte[] getData() {
        return data;
    }
}
