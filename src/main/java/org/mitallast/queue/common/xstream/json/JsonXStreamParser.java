package org.mitallast.queue.common.xstream.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.base.GeneratorBase;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.common.strings.CharReference;
import org.mitallast.queue.common.strings.Strings;
import org.mitallast.queue.common.xstream.XStreamType;
import org.mitallast.queue.common.xstream.support.AbstractXStreamParser;

import java.io.IOException;
import java.io.OutputStream;

public class JsonXStreamParser extends AbstractXStreamParser {

    protected final JsonParser parser;

    public JsonXStreamParser(JsonParser parser) {
        this.parser = parser;
    }

    private static Token convertToken(JsonToken token) {
        if (token == null) {
            return null;
        }
        switch (token) {
            case FIELD_NAME:
                return Token.FIELD_NAME;
            case VALUE_FALSE:
            case VALUE_TRUE:
                return Token.VALUE_BOOLEAN;
            case VALUE_STRING:
                return Token.VALUE_STRING;
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return Token.VALUE_NUMBER;
            case VALUE_NULL:
                return Token.VALUE_NULL;
            case START_OBJECT:
                return Token.START_OBJECT;
            case END_OBJECT:
                return Token.END_OBJECT;
            case START_ARRAY:
                return Token.START_ARRAY;
            case END_ARRAY:
                return Token.END_ARRAY;
            case VALUE_EMBEDDED_OBJECT:
                return Token.VALUE_EMBEDDED_OBJECT;
        }
        throw new IllegalStateException("No matching token for json_token [" + token + "]");
    }

    @Override
    public XStreamType contentType() {
        return XStreamType.JSON;
    }

    @Override
    public Token nextToken() throws IOException {
        return convertToken(parser.nextToken());
    }

    @Override
    public void skipChildren() throws IOException {
        parser.skipChildren();
    }

    @Override
    public Token currentToken() {
        return convertToken(parser.getCurrentToken());
    }

    @Override
    public String currentName() throws IOException {
        return parser.getCurrentName();
    }

    @Override
    public String text() throws IOException {
        return parser.getText();
    }

    @Override
    public ByteBuf utf8Bytes() throws IOException {
        return Unpooled.copiedBuffer(
            parser.getTextCharacters(),
            parser.getTextOffset(),
            parser.getTextLength(),
            Strings.UTF8
        );
    }

    @Override
    public CharSequence charSequence() throws IOException {
        return new CharReference(
            parser.getTextCharacters(),
            parser.getTextOffset(),
            parser.getTextLength()
        );
    }

    @Override
    public Object objectText() throws IOException {
        JsonToken currentToken = parser.getCurrentToken();
        if (currentToken == JsonToken.VALUE_STRING) {
            return text();
        } else if (currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
            return parser.getNumberValue();
        } else if (currentToken == JsonToken.VALUE_TRUE) {
            return Boolean.TRUE;
        } else if (currentToken == JsonToken.VALUE_FALSE) {
            return Boolean.FALSE;
        } else if (currentToken == JsonToken.VALUE_NULL) {
            return null;
        } else {
            return text();
        }
    }

    @Override
    public Object objectBytes() throws IOException {
        JsonToken currentToken = parser.getCurrentToken();
        if (currentToken == JsonToken.VALUE_STRING) {
            return utf8Bytes();
        } else if (currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
            return parser.getNumberValue();
        } else if (currentToken == JsonToken.VALUE_TRUE) {
            return Boolean.TRUE;
        } else if (currentToken == JsonToken.VALUE_FALSE) {
            return Boolean.FALSE;
        } else if (currentToken == JsonToken.VALUE_NULL) {
            return null;
        } else {
            //TODO should this really do UTF-8 conversion?
            return utf8Bytes();
        }
    }

    @Override
    public ByteBuf rawBytes() throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        readRawBytes(new ByteBufOutputStream(buffer));
        return buffer;
    }

    @Override
    public void readRawBytes(OutputStream outputStream) throws IOException {
        try (JsonGenerator generator = parser.getCodec().getFactory().createGenerator(outputStream)) {
            copyValue(generator);
        }
    }

    private void copyValue(JsonGenerator generator) throws IOException {
        JsonToken token = parser.getCurrentToken();
        switch (token) {
            case START_OBJECT:
                generator.writeStartObject();
                do {
                    token = parser.nextToken();
                    copyValue(generator);
                } while (token != JsonToken.END_OBJECT);
                break;
            case END_OBJECT:
                generator.writeEndObject();
                break;
            case START_ARRAY:
                generator.writeStartArray();
                do {
                    token = parser.nextToken();
                    copyValue(generator);
                } while (token != JsonToken.END_ARRAY);
                break;
            case END_ARRAY:
                generator.writeEndArray();
                break;
            case FIELD_NAME:
                generator.writeFieldName(parser.getCurrentName());
                parser.nextToken();
                copyValue(generator);
                break;
            case VALUE_STRING:
                if (parser.getParsingContext().inObject()) {
                    generator.writeRaw(':');
                }
                if (parser.getParsingContext().inArray() && parser.getParsingContext().getCurrentIndex() > 0) {
                    generator.writeRaw(',');
                }
                generator.writeRaw('"');
                generator.writeRaw(
                    parser.getTextCharacters(),
                    parser.getTextOffset(),
                    parser.getTextLength()
                );
                generator.writeRaw('"');
                ((GeneratorBase) generator).getOutputContext().writeValue();
                break;
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
            case VALUE_TRUE:
            case VALUE_FALSE:
            case VALUE_NULL:
                if (parser.getParsingContext().inObject()) {
                    generator.writeRaw(':');
                }
                if (parser.getParsingContext().inArray() && parser.getParsingContext().getCurrentIndex() > 0) {
                    generator.writeRaw(',');
                }
                generator.writeRaw(
                    parser.getTextCharacters(),
                    parser.getTextOffset(),
                    parser.getTextLength()
                );
                ((GeneratorBase) generator).getOutputContext().writeValue();
                break;
            default:
                throw new IllegalStateException("Unexpected token: " + token);
        }
    }

    @Override
    public boolean hasTextCharacters() {
        return parser.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return parser.getTextCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return parser.getTextLength();
    }

    @Override
    public int textOffset() throws IOException {
        return parser.getTextOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return parser.getNumberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return convertNumberType(parser.getNumberType());
    }

    @Override
    public boolean estimatedNumberType() {
        return true;
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return parser.getBinaryValue();
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return parser.getBooleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        return parser.getShortValue();
    }

    @Override
    protected int doIntValue() throws IOException {
        return parser.getIntValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        return parser.getLongValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        return parser.getFloatValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        return parser.getDoubleValue();
    }

    private NumberType convertNumberType(JsonParser.NumberType numberType) {
        switch (numberType) {
            case INT:
                return NumberType.INT;
            case LONG:
                return NumberType.LONG;
            case FLOAT:
                return NumberType.FLOAT;
            case DOUBLE:
                return NumberType.DOUBLE;
        }
        throw new IllegalStateException("No matching token for number_type [" + numberType + "]");
    }
}
