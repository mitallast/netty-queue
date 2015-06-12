package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessFileStreamOutput extends DataStreamOutput {

    private final RandomAccessFile file;
    private volatile long pos;

    public RandomAccessFileStreamOutput(RandomAccessFile file) {
        super(file);
        this.file = file;
    }

    private void savePos() throws IOException {
        pos = file.getChannel().position();
    }

    private void revertPos() throws IOException {
        file.getChannel().position(pos);
    }

    @Override
    public void write(int b) throws IOException {
        revertPos();
        try {
            super.write(b);
        } finally {
            savePos();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        revertPos();
        try {
            super.write(b);
        } finally {
            savePos();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        revertPos();
        try {
            super.write(b, off, len);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        revertPos();
        try {
            super.writeBoolean(v);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeByte(int v) throws IOException {
        revertPos();
        try {
            super.writeByte(v);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeShort(int v) throws IOException {
        revertPos();
        try {
            super.writeShort(v);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeChar(int v) throws IOException {
        revertPos();
        try {
            super.writeChar(v);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeInt(int v) throws IOException {
        revertPos();
        try {
            super.writeInt(v);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeLong(long v) throws IOException {
        revertPos();
        try {
            super.writeLong(v);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeFloat(float v) throws IOException {
        revertPos();
        try {
            super.writeFloat(v);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeDouble(double v) throws IOException {
        revertPos();
        try {
            super.writeDouble(v);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeByteBuf(ByteBuf buffer) throws IOException {
        revertPos();
        try {
            super.writeByteBuf(buffer);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeByteBuf(ByteBuf buffer, int length) throws IOException {
        revertPos();
        try {
            super.writeByteBuf(buffer, length);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeByteBufOrNull(ByteBuf buffer) throws IOException {
        revertPos();
        try {
            super.writeByteBufOrNull(buffer);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeByteBufOrNull(ByteBuf buffer, int length) throws IOException {
        revertPos();
        try {
            super.writeByteBufOrNull(buffer, length);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeBytes(String s) throws IOException {
        revertPos();
        try {
            super.writeBytes(s);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        revertPos();
        try {
            super.writeChars(s);
        } finally {
            savePos();
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        revertPos();
        try {
            super.writeUTF(s);
        } finally {
            savePos();
        }
    }
}
