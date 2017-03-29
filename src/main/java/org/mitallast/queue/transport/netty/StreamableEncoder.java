package org.mitallast.queue.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;

public class StreamableEncoder extends MessageToByteEncoder<Streamable> {
    private final StreamService streamService;

    public StreamableEncoder(StreamService streamService) {
        this.streamService = streamService;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Streamable message, ByteBuf out) throws Exception {
        int sizePos = out.writerIndex();
        out.writerIndex(out.writerIndex() + 4);
        try (StreamOutput output = streamService.output(out)) {
            output.writeClass(message.getClass());
            output.writeStreamable(message);
        }
        int size = out.writerIndex() - sizePos - 4;
        out.setInt(sizePos, size);
    }
}
