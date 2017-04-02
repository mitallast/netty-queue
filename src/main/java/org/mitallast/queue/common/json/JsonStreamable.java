package org.mitallast.queue.common.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.io.InputStream;

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
                return stream.readInt();
            }
        });
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        JsonService.mapper.writeValue(stream, json);
    }

    public JsonNode json() {
        return json;
    }
}
