package org.mitallast.queue.queue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

public class QueueMessage {

    public static final Charset defaultCharset = Charset.forName("UTF-8");

    private UUID uuid;
    private QueueMessageType type;
    private ByteBuf buffer;

    public QueueMessage() {
    }

    public QueueMessage(String source) {
        setSource(source);
    }

    public QueueMessage(UUID uuid, String source) {
        setUuid(uuid);
        setSource(source);
    }

    public QueueMessage(UUID uuid, QueueMessageType type, ByteBuf buffer) {
        this.uuid = uuid;
        this.type = type;
        this.buffer = buffer;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public QueueMessageType getMessageType() {
        return type;
    }

    public String getMessage() {
        return buffer.toString(defaultCharset);
    }

    public void setSource(String string) {
        type = QueueMessageType.STRING;
        buffer = Unpooled.wrappedBuffer(string.getBytes(defaultCharset));
    }

    public ByteBuf getSource() {
        return Unpooled.wrappedBuffer(buffer);
    }

    public void setSource(TreeNode tree) throws IOException {
        JsonFactory jsonFactory = new JsonFactory();
        if (buffer == null) {
            buffer = Unpooled.buffer();
        } else {
            buffer.clear();
        }
        type = QueueMessageType.JSON;
        try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer)) {
            JsonGenerator generator = jsonFactory.createGenerator(outputStream);
            generator.setCodec(new ObjectMapper());
            generator.writeTree(tree);
            generator.close();
        }
    }

    public void setSource(QueueMessageType type, ByteBuf buffer) {
        this.type = type;
        this.buffer = buffer;
    }

    public void writeTo(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        if (uuid != null) {
            generator.writeStringField("uuid", uuid.toString());
        }
        if (buffer != null) {
            buffer.resetReaderIndex();
            generator.writeFieldName("message");
            if (getMessageType() == QueueMessageType.STRING) {
                generator.writeString(buffer.toString(defaultCharset));
            } else if (getMessageType() == QueueMessageType.JSON) {
                while (buffer.isReadable()) {
                    generator.writeRaw(buffer.readChar());
                }
            }
        }
        generator.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueMessage that = (QueueMessage) o;

        if (buffer != null ? !ByteBufUtil.equals(buffer, that.buffer) : that.buffer != null) return false;
        if (type != that.type) return false;
        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (buffer != null ? ByteBufUtil.hashCode(buffer) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "QueueMessage{" +
            "uuid=" + uuid +
            ", type=" + type +
            ", buffer=" + buffer.toString(defaultCharset) +
            '}';
    }
}
