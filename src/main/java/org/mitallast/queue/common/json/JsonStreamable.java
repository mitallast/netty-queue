package org.mitallast.queue.common.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonStreamable implements Streamable {
    private final JsonNode json;

    public JsonStreamable(JsonNode json) {
        Preconditions.checkNotNull(json);
        this.json = json;
    }

    public JsonStreamable(StreamInput stream) throws IOException {
        json = JsonService.mapper.readTree(new InputStream() {
            @Override
            public int read() throws IOException {
                return stream.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                stream.read(b, off, len);
                return len;
            }
        });
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        JsonService.mapper.writeValue(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                stream.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                stream.write(b, off, len);
            }
        }, json);
    }

    public JsonNode json() {
        return json;
    }
}
