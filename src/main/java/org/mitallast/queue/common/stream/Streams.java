package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.*;

public class Streams {

    public static StreamInput input(ByteBuf buffer) {
        return new ByteBufStreamInput(buffer);
    }

    public static StreamInput input(ByteBuf buffer, int size) {
        return new ByteBufStreamInput(buffer, size);
    }

    public static StreamInput input(RandomAccessFile data) throws IOException {
        return new RandomAccessFileStreamInput(data);
    }

    public static StreamInput input(File file) throws IOException {
        return input(new FileInputStream(file));
    }

    public static StreamInput input(InputStream inputStream) throws IOException {
        return input((DataInput) new DataInputStream(inputStream));
    }

    public static StreamInput input(DataInput dataInput) throws IOException {
        return new DataStreamInput(dataInput);
    }

    public static StreamOutput output(ByteBuf buffer) {
        return new ByteBufStreamOutput(buffer);
    }

    public static StreamOutput output(RandomAccessFile data) throws IOException {
        return new RandomAccessFileStreamOutput(data);
    }

    public static StreamOutput output(File file) throws IOException {
        return output(new FileOutputStream(file));
    }

    public static StreamOutput output(OutputStream outputStream) throws IOException {
        return output((DataOutput) new DataOutputStream(outputStream));
    }

    public static StreamOutput output(DataOutput dataOutput) throws IOException {
        return new DataStreamOutput(dataOutput);
    }
}
