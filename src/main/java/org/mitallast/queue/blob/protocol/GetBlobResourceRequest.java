package org.mitallast.queue.blob.protocol;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;

public class GetBlobResourceRequest implements Streamable {
    private final long id;
    private final String key;

    public GetBlobResourceRequest(long id, String key) {
        this.id = id;
        this.key = key;
    }

    public GetBlobResourceRequest(StreamInput stream) throws IOException {
        id = stream.readLong();
        key = stream.readText();
    }

    public long getId() {
        return id;
    }

    public String getKey() {
        return key;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeLong(id);
        stream.writeText(key);
    }
}
