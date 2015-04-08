package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import org.mitallast.queue.common.settings.Settings;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class ByteBufStreamOutput extends ByteBufOutputStream implements StreamOutput {

    private final ByteBuf buffer;

    public ByteBufStreamOutput(ByteBuf buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    @Override
    public void writeText(String text) throws IOException {
        writeUTF(text);
    }

    @Override
    public void writeTextOrNull(String text) throws IOException {
        if (text == null || text.isEmpty()) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeUTF(text);
        }
    }

    @Override
    public void writeUUID(UUID uuid) throws IOException {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    @Override
    public void writeUUIDOrNull(UUID uuid) throws IOException {
        if (uuid != null) {
            writeLong(uuid.getMostSignificantBits());
            writeLong(uuid.getLeastSignificantBits());
        } else {
            writeLong(0);
            writeLong(0);
        }
    }

    @Override
    public <Type extends Enum<Type>> void writeEnum(Type type) throws IOException {
        writeInt(type.ordinal());
    }

    @Override
    public <Type extends Enum<Type>> void writeEnumOrNull(Type type) throws IOException {
        if (type != null) {
            writeInt(type.ordinal());
        } else {
            writeInt(-1);
        }
    }

    @Override
    public void writeByteBuf(ByteBuf buffer) throws IOException {
        int length = buffer.readableBytes();
        writeInt(length);
        if (length > 0) {
            this.buffer.ensureWritable(length);
            buffer.readBytes(this.buffer, length);
        }
    }

    @Override
    public void writeByteBuf(ByteBuf buffer, int length) throws IOException {
        writeInt(length);
        if (length > 0) {
            this.buffer.ensureWritable(length);
            buffer.readBytes(this.buffer, length);
        }
    }

    @Override
    public void writeByteBufOrNull(ByteBuf buffer) throws IOException {
        if (buffer == null) {
            writeInt(-1);
        } else {
            int length = buffer.readableBytes();
            writeInt(length);
            if (length > 0) {
                this.buffer.ensureWritable(length);
                buffer.readBytes(this.buffer, length);
            }
        }
    }

    @Override
    public void writeByteBufOrNull(ByteBuf buffer, int length) throws IOException {
        if (buffer == null) {
            writeInt(-1);
        } else {
            writeInt(length);
            if (length > 0) {
                this.buffer.ensureWritable(length);
                buffer.readBytes(this.buffer, length);
            }
        }
    }

    @Override
    public void writeSettings(Settings settings) throws IOException {
        Map<String, String> asMap = settings.getAsMap();
        buffer.writeInt(asMap.size());
        for (Map.Entry<String, String> entry : asMap.entrySet()) {
            writeUTF(entry.getKey());
            writeUTF(entry.getValue());
        }
    }
}
