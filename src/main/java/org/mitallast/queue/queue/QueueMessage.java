package org.mitallast.queue.queue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;

public class QueueMessage {

    public static final Charset defaultCharset = Charset.forName("UTF-8");
    private static final JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());

    public UUID uuid;
    private byte[] source;

    public QueueMessage() {
    }

    public QueueMessage(UUID uuid, byte[] message) {
        setUuid(uuid);
        setSource(message);
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public void setUuid(String uuid) {
        setUuid(UUID.fromString(uuid));
    }

    public QueueMessageType getMessageType() {
        int sourceType = source[0];
        return QueueMessageType.values()[sourceType];
    }

    public String getMessage() {
        return new String(source, 1, source.length - 1, defaultCharset);
    }

    public void setSource(String string) {
        byte[] sourceString = string.getBytes(defaultCharset);
        source = new byte[sourceString.length + 1];
        source[0] = (byte) QueueMessageType.STRING.ordinal();
        System.arraycopy(sourceString, 0, source, 1, sourceString.length);
    }

    public void setSource(TreeNode tree) throws IOException {
        JsonFactory jsonFactory = new JsonFactory();
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(512)) {
            outputStream.write(QueueMessageType.JSON.ordinal());
            JsonGenerator generator = jsonFactory.createGenerator(outputStream);
            generator.setCodec(new ObjectMapper());
            generator.writeTree(tree);
            generator.close();
            source = outputStream.toByteArray();
        }
    }

    public byte[] getSource() {
        return source;
    }

    public void setSource(byte[] newSource) {
        source = newSource;
    }

    public void setSource(QueueMessageType type, ByteBuf buffer) {
        source = new byte[1 + buffer.readableBytes()];
        source[0] = (byte) type.ordinal();
        buffer.readBytes(source, 1, source.length - 1);
    }

    public InputStream getSourceAsStream() throws IOException {
        return new ByteArrayInputStream(source, 1, source.length - 1);
    }

    public TreeNode getSourceAsTreeNode() throws IOException {
        JsonParser parser = jsonFactory.createParser(getSourceAsStream());
        return parser.readValueAsTree();
    }

    public void writeTo(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        if (uuid != null) {
            generator.writeStringField("uuid", uuid.toString());
        }
        if (source != null) {
            generator.writeFieldName("message");
            if (getMessageType() == QueueMessageType.STRING) {
                generator.writeRawUTF8String(source, 1, source.length - 1);
            } else if (getMessageType() == QueueMessageType.JSON) {
                generator.writeTree(getSourceAsTreeNode());
            }
        }
        generator.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueMessage that = (QueueMessage) o;

        if (!Arrays.equals(source, that.source)) return false;
        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (source != null ? Arrays.hashCode(source) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "QueueMessage{" +
            "uuid=" + uuid +
            ", source=" + Arrays.toString(source) +
            '}';
    }
}
