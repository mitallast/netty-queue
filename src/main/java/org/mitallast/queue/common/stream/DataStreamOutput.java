package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public class DataStreamOutput implements StreamOutput {

    private final DataOutput output;
    private OutputStream outputStream;

    public DataStreamOutput(DataOutput output) {
        this.output = output;
    }

    private OutputStream outputStream() {
        if (outputStream == null) {
            outputStream = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    output.write(b);
                }

                @Override
                public void write(byte[] b) throws IOException {
                    output.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    output.write(b, off, len);
                }
            };
        }
        return outputStream;
    }

    @Override
    public void write(int b) throws IOException {
        output.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        output.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        output.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        output.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        output.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        output.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        output.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        output.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        output.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        output.writeDouble(v);
    }

    @Override
    public void writeByteBuf(ByteBuf buffer) throws IOException {
        writeByteBuf(buffer, buffer.readableBytes());
    }

    @Override
    public void writeByteBuf(ByteBuf buffer, int length) throws IOException {
        writeInt(length);
        if (length > 0) {
            buffer.readBytes(outputStream, length);
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
                buffer.readBytes(outputStream, length);
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
                buffer.readBytes(outputStream, length);
            }
        }
    }

    @Override
    public void writeBytes(String s) throws IOException {
        output.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        output.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        output.writeUTF(s);
    }

    @Override
    public void close() throws IOException {

    }
}
