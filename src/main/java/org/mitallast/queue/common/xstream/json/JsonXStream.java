package org.mitallast.queue.common.xstream.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.mitallast.queue.common.xstream.XStream;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.common.xstream.XStreamParser;
import org.mitallast.queue.common.xstream.XStreamType;

import java.io.*;

public class JsonXStream implements XStream {

    public final static JsonXStream jsonXContent;

    protected final static JsonFactory jsonFactory;

    static {
        jsonFactory = new JsonFactory(new ObjectMapper());
        jsonFactory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        jsonFactory.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
        jsonFactory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        jsonFactory.configure(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW, false);
        jsonXContent = new JsonXStream();
    }

    private JsonXStream() {
    }

    @Override
    public XStreamType type() {
        return XStreamType.JSON;
    }

    @Override
    public XStreamBuilder createGenerator(OutputStream os) throws IOException {
        return new JsonXStreamBuilder(jsonFactory.createGenerator(os, JsonEncoding.UTF8));
    }

    @Override
    public XStreamBuilder createGenerator(Writer writer) throws IOException {
        return new JsonXStreamBuilder(jsonFactory.createGenerator(writer));
    }

    @Override
    public XStreamBuilder createGenerator(ByteBuf buffer) throws IOException {
        return createGenerator(new ByteBufOutputStream(buffer));
    }

    @Override
    public XStreamParser createParser(String content) throws IOException {
        return new JsonXStreamParser(jsonFactory.createParser(content));
    }

    @Override
    public XStreamParser createParser(InputStream is) throws IOException {
        return new JsonXStreamParser(jsonFactory.createParser(is));
    }

    @Override
    public XStreamParser createParser(byte[] data) throws IOException {
        return new JsonXStreamParser(jsonFactory.createParser(data));
    }

    @Override
    public XStreamParser createParser(byte[] data, int offset, int length) throws IOException {
        return new JsonXStreamParser(jsonFactory.createParser(data, offset, length));
    }

    @Override
    public XStreamParser createParser(ByteBuf bytes) throws IOException {
        if (bytes.hasArray()) {
            return createParser(bytes.array(), bytes.arrayOffset(), bytes.readableBytes());
        }
        return createParser(new ByteBufInputStream(bytes));
    }

    @Override
    public XStreamParser createParser(Reader reader) throws IOException {
        return new JsonXStreamParser(jsonFactory.createParser(reader));
    }
}
