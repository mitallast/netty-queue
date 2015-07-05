package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.*;
import java.util.function.Supplier;

public interface StreamService {

    <T extends Streamable> void registerClass(Class<T> streamableClass, Supplier<T> supplier, int id);

    StreamInput input(ByteBuf buffer);

    StreamInput input(ByteBuf buffer, int size);

    StreamInput input(File file) throws IOException;

    StreamInput input(InputStream inputStream) throws IOException;

    StreamInput input(DataInput dataInput) throws IOException;

    StreamOutput output(ByteBuf buffer);

    StreamOutput output(File file) throws IOException;

    StreamOutput output(OutputStream outputStream) throws IOException;

    StreamOutput output(DataOutput dataOutput) throws IOException;
}
