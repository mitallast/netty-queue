package org.mitallast.queue.common.xstream;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public interface XStreamBuilder extends Closeable {

    XStreamType contentType();

    XStreamBuilder usePrettyPrint();

    XStreamBuilder writeStartArray() throws IOException;

    XStreamBuilder writeEndArray() throws IOException;

    XStreamBuilder writeStartObject() throws IOException;

    XStreamBuilder writeEndObject() throws IOException;

    XStreamBuilder writeFieldName(String name) throws IOException;

    XStreamBuilder writeFieldName(XStreamString name) throws IOException;

    XStreamBuilder writeString(String text) throws IOException;

    XStreamBuilder writeString(XStreamString text) throws IOException;

    XStreamBuilder writeString(char[] text, int offset, int len) throws IOException;

    XStreamBuilder writeUTF8String(byte[] text, int offset, int length) throws IOException;

    XStreamBuilder writeBinary(byte[] data, int offset, int len) throws IOException;

    XStreamBuilder writeBinary(byte[] data) throws IOException;

    XStreamBuilder writeNumber(int v) throws IOException;

    XStreamBuilder writeNumber(long v) throws IOException;

    XStreamBuilder writeNumber(double d) throws IOException;

    XStreamBuilder writeNumber(float f) throws IOException;

    XStreamBuilder writeBoolean(boolean state) throws IOException;

    XStreamBuilder writeNull() throws IOException;

    XStreamBuilder writeStringField(String fieldName, String value) throws IOException;

    XStreamBuilder writeStringField(XStreamString fieldName, String value) throws IOException;

    XStreamBuilder writeBooleanField(String fieldName, boolean value) throws IOException;

    XStreamBuilder writeBooleanField(XStreamString fieldName, boolean value) throws IOException;

    XStreamBuilder writeNullField(String fieldName) throws IOException;

    XStreamBuilder writeNullField(XStreamString fieldName) throws IOException;

    XStreamBuilder writeNumberField(String fieldName, int value) throws IOException;

    XStreamBuilder writeNumberField(XStreamString fieldName, int value) throws IOException;

    XStreamBuilder writeNumberField(String fieldName, long value) throws IOException;

    XStreamBuilder writeNumberField(XStreamString fieldName, long value) throws IOException;

    XStreamBuilder writeNumberField(String fieldName, double value) throws IOException;

    XStreamBuilder writeNumberField(XStreamString fieldName, double value) throws IOException;

    XStreamBuilder writeNumberField(String fieldName, float value) throws IOException;

    XStreamBuilder writeNumberField(XStreamString fieldName, float value) throws IOException;

    XStreamBuilder writeBinaryField(String fieldName, byte[] data) throws IOException;

    XStreamBuilder writeBinaryField(XStreamString fieldName, byte[] data) throws IOException;

    XStreamBuilder writeArrayFieldStart(String fieldName) throws IOException;

    XStreamBuilder writeArrayFieldStart(XStreamString fieldName) throws IOException;

    XStreamBuilder writeObjectFieldStart(String fieldName) throws IOException;

    XStreamBuilder writeObjectFieldStart(XStreamString fieldName) throws IOException;

    XStreamBuilder writeRawField(String fieldName, byte[] content) throws IOException;

    XStreamBuilder writeRawField(String fieldName, byte[] content, int offset, int length) throws IOException;

    XStreamBuilder writeRawField(String fieldName, InputStream content) throws IOException;

    XStreamBuilder writeRawField(String fieldName, ByteBuf content) throws IOException;

    XStreamBuilder copyCurrentStructure(XStreamParser parser) throws IOException;

    XStreamBuilder flush() throws IOException;
}
