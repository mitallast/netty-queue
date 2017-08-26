package org.mitallast.queue.common.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;

import static org.mitallast.queue.common.json.JsonService.mapper;

public class JsonMessage implements Message {
    public static final Codec<JsonMessage> codec = new Codec<JsonMessage>() {
        @Override
        public JsonMessage read(DataInput stream) {
            try {
                JsonParser parser = mapper.getFactory().createParser(stream);
                JsonNode json = mapper.readTree(parser);
                return new JsonMessage(json);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void write(DataOutput stream, JsonMessage value) {
            try {
                JsonGenerator generator = mapper.getFactory().createGenerator(stream);
                mapper.writeValue(generator, value.json());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    };

    private final JsonNode json;

    public JsonMessage(String json) {
        try {
            this.json = mapper.readTree(json);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public JsonMessage(JsonNode json) {
        Preconditions.checkNotNull(json);
        this.json = json;
    }

    public JsonNode json() {
        return json;
    }
}
