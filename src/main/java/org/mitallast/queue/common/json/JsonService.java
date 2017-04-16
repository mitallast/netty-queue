package org.mitallast.queue.common.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonService {

    protected final static ObjectMapper mapper;

    static {
        SimpleModule module = new SimpleModule();
        module.addSerializer(Config.class, new ConfigSerializer());
        module.addDeserializer(Config.class, new ConfigDeserializer());
        module.addSerializer(JsonStreamable.class, new JsonStreamableSerializer());
        module.addDeserializer(JsonStreamable.class, new JsonStreamableDeserializer());

        mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.registerModule(module);
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JodaModule());
    }

    public void serialize(ByteBuf buf, Object json) {
        try (OutputStream out = new ByteBufOutputStream(buf)) {
            serialize(out, json);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String serialize(Object json) {
        try {
            return mapper.writeValueAsString(json);
        } catch (JsonProcessingException e) {
            throw new JsonException(e);
        }
    }

    public void serialize(OutputStream out, Object json) {
        try {
            mapper.writeValue(out, json);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public <T> T deserialize(String data, Class<T> type) {
        return deserialize(data, type);
    }

    public <T> T deserialize(ByteBuf buf, Class<T> type) {
        try (InputStream input = new ByteBufInputStream(buf)) {
            return deserialize(input, type);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public <T> T deserialize(InputStream input, Class<T> type) {
        try {
            return mapper.readValue(input, type);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public <T> T deserialize(String data, TypeReference<T> type) {
        try {
            return mapper.readValue(data, type);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public <T> T deserialize(ByteBuf buf, TypeReference<T> type) {
        try (InputStream input = new ByteBufInputStream(buf)) {
            return deserialize(input, type);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public <T> T deserialize(InputStream input, TypeReference<T> type) {
        try {
            return mapper.readValue(input, type);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    private static class ConfigSerializer extends JsonSerializer<Config> {

        @Override
        public void serialize(Config value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            String render = value.root().render(ConfigRenderOptions.concise());
            gen.writeRawValue(render);
        }
    }

    private static class ConfigDeserializer extends JsonDeserializer<Config> {

        @Override
        public Config deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String json = p.readValueAsTree().toString();
            return ConfigFactory.parseString(json);
        }
    }

    private static class JsonStreamableSerializer extends JsonSerializer<JsonStreamable> {

        @Override
        public void serialize(JsonStreamable value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeTree(value.json());
        }
    }

    private static class JsonStreamableDeserializer extends JsonDeserializer<JsonStreamable> {
        @Override
        public JsonStreamable deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode treeNode = p.readValueAsTree();
            return new JsonStreamable(treeNode);
        }
    }
}
