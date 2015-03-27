package org.mitallast.queue.common.xstream.support;

import org.mitallast.queue.common.xstream.XStreamBuilder;

import java.io.IOException;

public abstract class AbstractXStreamBuilder implements XStreamBuilder {

    @Override
    public XStreamBuilder writeStringField(String fieldName, String value) throws IOException {
        writeFieldName(fieldName);
        writeString(value);
        return this;
    }

    @Override
    public XStreamBuilder writeBooleanField(String fieldName, boolean value) throws IOException {
        writeFieldName(fieldName);
        writeBoolean(value);
        return this;
    }

    @Override
    public XStreamBuilder writeNullField(String fieldName) throws IOException {
        writeFieldName(fieldName);
        writeNull();
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(String fieldName, int value) throws IOException {
        writeFieldName(fieldName);
        writeNumber(value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(String fieldName, long value) throws IOException {
        writeFieldName(fieldName);
        writeNumber(value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(String fieldName, double value) throws IOException {
        writeFieldName(fieldName);
        writeNumber(value);
        return this;
    }

    @Override
    public XStreamBuilder writeNumberField(String fieldName, float value) throws IOException {
        writeFieldName(fieldName);
        writeNumber(value);
        return this;
    }

    @Override
    public XStreamBuilder writeBinaryField(String fieldName, byte[] data) throws IOException {
        writeFieldName(fieldName);
        writeBinary(data);
        return this;
    }

    @Override
    public XStreamBuilder writeArrayFieldStart(String fieldName) throws IOException {
        writeFieldName(fieldName);
        writeStartArray();
        return this;
    }

    @Override
    public XStreamBuilder writeObjectFieldStart(String fieldName) throws IOException {
        writeFieldName(fieldName);
        writeStartObject();
        return this;
    }
}
