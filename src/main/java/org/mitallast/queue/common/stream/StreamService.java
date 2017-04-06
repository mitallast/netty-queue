package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.*;

public interface StreamService {

    StreamInput input(ByteBuf buffer);

    StreamInput input(File file) throws IOException;

    StreamOutput output(ByteBuf buffer);

    StreamOutput output(File file) throws IOException;

    StreamOutput output(File file, boolean append) throws IOException;
}
