package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessFileStreamInput extends DataStreamInput {

    private final RandomAccessFile file;
    private volatile long pos;

    public RandomAccessFileStreamInput(RandomAccessFile file) {
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
    public void readFully(byte[] b) throws IOException {
        revertPos();
        try {
            super.readFully(b);
        } finally {
            savePos();
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        revertPos();
        try {
            super.readFully(b, off, len);
        } finally {
            savePos();
        }
    }

    @Override
    public int skipBytes(int n) throws IOException {
        revertPos();
        try {
            return super.skipBytes(n);
        } finally {
            savePos();
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        revertPos();
        try {
            return super.readBoolean();
        } finally {
            savePos();
        }
    }

    @Override
    public byte readByte() throws IOException {
        revertPos();
        try {
            return super.readByte();
        } finally {
            savePos();
        }
    }

    @Override
    public int readUnsignedByte() throws IOException {
        revertPos();
        try {
            return super.readUnsignedByte();
        } finally {
            savePos();
        }
    }

    @Override
    public short readShort() throws IOException {
        revertPos();
        try {
            return super.readShort();
        } finally {
            savePos();
        }
    }

    @Override
    public int readUnsignedShort() throws IOException {
        revertPos();
        try {
            return super.readUnsignedShort();
        } finally {
            savePos();
        }
    }

    @Override
    public char readChar() throws IOException {
        revertPos();
        try {
            return super.readChar();
        } finally {
            savePos();
        }
    }

    @Override
    public int readInt() throws IOException {
        revertPos();
        try {
            return super.readInt();
        } finally {
            savePos();
        }
    }

    @Override
    public long readLong() throws IOException {
        revertPos();
        try {
            return super.readLong();
        } finally {
            savePos();
        }
    }

    @Override
    public float readFloat() throws IOException {
        revertPos();
        try {
            return super.readFloat();
        } finally {
            savePos();
        }
    }

    @Override
    public double readDouble() throws IOException {
        revertPos();
        try {
            return super.readDouble();
        } finally {
            savePos();
        }
    }

    @Override
    public String readLine() throws IOException {
        revertPos();
        try {
            return super.readLine();
        } finally {
            savePos();
        }
    }

    @Override
    public String readUTF() throws IOException {
        revertPos();
        try {
            return super.readUTF();
        } finally {
            savePos();
        }
    }

    @Override
    public ByteBuf readByteBuf() throws IOException {
        revertPos();
        try {
            return super.readByteBuf();
        } finally {
            savePos();
        }
    }

    @Override
    public ByteBuf readByteBufOrNull() throws IOException {
        revertPos();
        try {
            return super.readByteBufOrNull();
        } finally {
            savePos();
        }
    }
}
