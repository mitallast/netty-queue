package org.mitallast.queue.common.xstream.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.base.GeneratorBase;
import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.Streams;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.common.xstream.XStreamParser;
import org.mitallast.queue.common.xstream.XStreamString;
import org.mitallast.queue.common.xstream.XStreamType;
import org.mitallast.queue.common.xstream.support.AbstractXStreamBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonXStreamBuilder extends AbstractXStreamBuilder {

    private final JsonGenerator generator;
    private final GeneratorBase base;

    public JsonXStreamBuilder(JsonGenerator generator) {
        this.generator = generator;
        if (generator instanceof GeneratorBase) {
            base = (GeneratorBase) generator;
        } else {
            base = null;
        }
    }

    @Override
    public XStreamType contentType() {
        return XStreamType.JSON;
    }

    @Override
    public XStreamBuilder usePrettyPrint() {
        generator.useDefaultPrettyPrinter();
        return this;
    }

    @Override
    public XStreamBuilder writeStartArray() throws IOException {
        generator.writeStartArray();
        return this;
    }

    @Override
    public XStreamBuilder writeEndArray() throws IOException {
        generator.writeEndArray();
        return this;
    }

    @Override
    public XStreamBuilder writeStartObject() throws IOException {
        generator.writeStartObject();
        return this;
    }

    @Override
    public XStreamBuilder writeEndObject() throws IOException {
        generator.writeEndObject();
        return this;
    }

    @Override
    public XStreamBuilder writeFieldName(String name) throws IOException {
        generator.writeFieldName(name);
        return this;
    }

    @Override
    public XStreamBuilder writeFieldName(XStreamString name) throws IOException {
        generator.writeFieldName(name);
        return this;
    }

    @Override
    public XStreamBuilder writeString(String text) throws IOException {
        generator.writeString(text);
        return this;
    }

    @Override
    public XStreamBuilder writeString(char[] text, int offset, int len) throws IOException {
        generator.writeString(text, offset, len);
        return this;
    }

    @Override
    public XStreamBuilder writeUTF8String(byte[] text, int offset, int length) throws IOException {
        generator.writeUTF8String(text, offset, length);
        return this;
    }

    @Override
    public XStreamBuilder writeBinary(byte[] data, int offset, int len) throws IOException {
        generator.writeBinary(data, offset, len);
        return this;
    }

    @Override
    public XStreamBuilder writeBinary(byte[] data) throws IOException {
        generator.writeBinary(data);
        return this;
    }

    @Override
    public XStreamBuilder writeNumber(int v) throws IOException {
        generator.writeNumber(v);
        return this;
    }

    @Override
    public XStreamBuilder writeNumber(long v) throws IOException {
        generator.writeNumber(v);
        return this;
    }

    @Override
    public XStreamBuilder writeNumber(double d) throws IOException {
        generator.writeNumber(d);
        return this;
    }

    @Override
    public XStreamBuilder writeNumber(float f) throws IOException {
        generator.writeNumber(f);
        return this;
    }

    @Override
    public XStreamBuilder writeBoolean(boolean state) throws IOException {
        generator.writeBoolean(state);
        return this;
    }

    @Override
    public XStreamBuilder writeNull() throws IOException {
        generator.writeNull();
        return this;
    }

    @Override
    public XStreamBuilder writeStringField(String fieldName, String value) throws IOException {
        generator.writeStringField(fieldName, value);
        return this;
    }

    @Override
    public XStreamBuilder writeStringField(XStreamString fieldName, String value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeString(value);
        return this;
    }

    @Override
    public XStreamBuilder writeBooleanField(String fieldName, boolean value) throws IOException {
        generator.writeBooleanField(fieldName, value);
        return this;
    }

    @Override
    public XStreamBuilder writeBooleanField(XStreamString fieldName, boolean value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeBoolean(value);
        return this;
    }

    @Override
    public XStreamBuilder writeNullField(String fieldName) throws IOException {
        generator.writeNullField(fieldName);
        return this;
    }

    @Override
    public XStreamBuilder writeNullField(XStreamString fieldName) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNull();
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(String fieldName, int value) throws IOException {
        generator.writeNumberField(fieldName, value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(XStreamString fieldName, int value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNumber(value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(String fieldName, long value) throws IOException {
        generator.writeNumberField(fieldName, value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(XStreamString fieldName, long value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNumber(value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(String fieldName, double value) throws IOException {
        generator.writeNumberField(fieldName, value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(XStreamString fieldName, double value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNumber(value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(String fieldName, float value) throws IOException {
        generator.writeNumberField(fieldName, value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(XStreamString fieldName, float value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeNumber(value);
        return this;
    }

    @Override
    public XStreamBuilder writeBinaryField(String fieldName, byte[] data) throws IOException {
        generator.writeBinaryField(fieldName, data);
        return this;
    }

    @Override
    public XStreamBuilder writeBinaryField(XStreamString fieldName, byte[] value) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeBinary(value);
        return this;
    }

    @Override
    public XStreamBuilder writeArrayFieldStart(String fieldName) throws IOException {
        generator.writeArrayFieldStart(fieldName);
        return this;
    }

    @Override
    public XStreamBuilder writeArrayFieldStart(XStreamString fieldName) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeStartArray();
        return this;
    }

    @Override
    public XStreamBuilder writeObjectFieldStart(String fieldName) throws IOException {
        generator.writeObjectFieldStart(fieldName);
        return this;
    }

    @Override
    public XStreamBuilder writeObjectFieldStart(XStreamString fieldName) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeStartObject();
        return this;
    }

    @Override
    public XStreamBuilder writeRawField(String fieldName, byte[] content, OutputStream bos) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeRaw(':');
        flush();
        bos.write(content);
        finishWriteRaw();
        return this;
    }

    @Override
    public XStreamBuilder writeRawField(String fieldName, byte[] content, int offset, int length, OutputStream bos) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeRaw(':');
        flush();
        bos.write(content, offset, length);
        finishWriteRaw();
        return this;
    }

    @Override
    public XStreamBuilder writeRawField(String fieldName, InputStream content, OutputStream bos) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeRaw(':');
        flush();
        Streams.copy(content, bos);
        finishWriteRaw();
        return this;
    }

    @Override
    public final XStreamBuilder writeRawField(String fieldName, ByteBuf content, OutputStream bos) throws IOException {
        generator.writeFieldName(fieldName);
        generator.writeRaw(':');
        flush();
        if (content.hasArray()) {
            content.array();
            bos.write(content.array(), content.arrayOffset(), content.readableBytes());
        } else {
            for (int i = content.readableBytes(); i > 0; i--) {
                bos.write(content.readByte());
            }
        }
        finishWriteRaw();
        return this;
    }

    @SuppressWarnings("ConstantConditions")
    private XStreamBuilder finishWriteRaw() {
        assert base != null : "JsonGenerator should be of instance GeneratorBase but was: " + generator.getClass();
        if (base != null) {
            base.getOutputContext().writeValue();
        }
        return this;
    }

    @Override
    public XStreamBuilder copyCurrentStructure(XStreamParser parser) throws IOException {
        // the start of the parser
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser instanceof JsonXStreamParser) {
            generator.copyCurrentStructure(((JsonXStreamParser) parser).parser);
        } else {
            throw new IOException("unsupported");
        }
        return this;
    }

    @Override
    public XStreamBuilder flush() throws IOException {
        generator.flush();
        return this;
    }

    @Override
    public void close() throws IOException {
        if (generator.isClosed()) {
            return;
        }
        generator.close();
    }
}
