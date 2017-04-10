package org.mitallast.queue.common.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.DataOutput;
import java.io.IOException;

public class JsonStreamable implements Streamable {
    private final JsonNode json;

    public JsonStreamable(String json) throws IOException {
        this.json = JsonService.mapper.readTree(json);
    }

    public JsonStreamable(JsonNode json) {
        Preconditions.checkNotNull(json);
        this.json = json;
    }

    public JsonStreamable(StreamInput stream) throws IOException {
        ByteBuf buffer = stream.readByteBuf();
        try {
            this.json = JsonService.mapper.readTree(new ByteBufInputStream(buffer));
        } finally {
            buffer.release();
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            JsonService.mapper.writeValue((DataOutput) new ByteBufOutputStream(buffer), json);
            stream.writeByteBuf(buffer);
        } finally {
            buffer.release();
        }
    }

    public JsonNode json() {
        return json;
    }
}
