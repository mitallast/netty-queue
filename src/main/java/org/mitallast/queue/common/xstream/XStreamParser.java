package org.mitallast.queue.common.xstream;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public interface XStreamParser extends Closeable {

    XStreamType contentType();

    Token nextToken() throws IOException;

    void skipChildren() throws IOException;

    Token currentToken();

    String currentName() throws IOException;

    String text() throws IOException;

    String textOrNull() throws IOException;

    ByteBuf utf8BytesOrNull() throws IOException;

    ByteBuf utf8Bytes() throws IOException;

    CharSequence charSequenceOrNull() throws IOException;

    CharSequence charSequence() throws IOException;

    Object objectText() throws IOException;

    Object objectBytes() throws IOException;

    ByteBuf rawBytes() throws IOException;

    void readRawBytes(OutputStream outputStream) throws IOException;

    boolean hasTextCharacters();

    char[] textCharacters() throws IOException;

    int textLength() throws IOException;

    int textOffset() throws IOException;

    Number numberValue() throws IOException;

    NumberType numberType() throws IOException;

    boolean estimatedNumberType();

    short shortValue(boolean coerce) throws IOException;

    int intValue(boolean coerce) throws IOException;

    long longValue(boolean coerce) throws IOException;

    float floatValue(boolean coerce) throws IOException;

    double doubleValue(boolean coerce) throws IOException;

    short shortValue() throws IOException;

    int intValue() throws IOException;

    long longValue() throws IOException;

    float floatValue() throws IOException;

    double doubleValue() throws IOException;

    boolean isBooleanValue() throws IOException;

    boolean booleanValue() throws IOException;

    byte[] binaryValue() throws IOException;

    public enum Token {
        FIELD_NAME,
        VALUE_BOOLEAN,
        VALUE_STRING,
        VALUE_NUMBER,
        VALUE_NULL,
        START_OBJECT,
        END_OBJECT,
        START_ARRAY,
        END_ARRAY,
        token, VALUE_EMBEDDED_OBJECT
    }

    enum NumberType {
        INT, LONG, FLOAT, DOUBLE
    }
}
