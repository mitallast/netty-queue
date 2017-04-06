package org.mitallast.queue.blob.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class GetBlobResourceResponse implements Streamable {
    private final long id;
    private final String key;
    private final byte[] data;

    public GetBlobResourceResponse(long id, String key, byte[] data) {
        this.id = id;
        this.key = key;
        this.data = data;
    }

    public GetBlobResourceResponse(StreamInput stream) throws IOException {
        id = stream.readLong();
        key = stream.readText();
        int size = stream.readInt();
        data = new byte[size];
        stream.read(data);
    }

    public long getId() {
        return id;
    }

    public String getKey() {
        return key;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(id);
        stream.writeText(key);
        stream.writeInt(data.length);
        stream.write(data);
    }
}
