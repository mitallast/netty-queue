package org.mitallast.queue.common.stream;

import io.netty.buffer.ByteBuf;

import java.io.File;

public interface StreamService {

    StreamInput input(ByteBuf buffer);

    StreamInput input(File file);

    StreamOutput output(ByteBuf buffer);

    StreamOutput output(File file);

    StreamOutput output(File file, boolean append);
}
