package org.mitallast.queue.transport.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;

public class TransportFrameEncoder extends MessageToByteEncoder<TransportFrame> {

    private final StreamService streamService;

    public TransportFrameEncoder(StreamService streamService) {
        this.streamService = streamService;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, TransportFrame frame, ByteBuf out) throws Exception {
        out.writeByte('E');
        out.writeByte('Q');
        out.writeInt(frame.version().id);
        out.writeLong(frame.request());

        if (frame instanceof StreamableTransportFrame) {
            // message request
            // skip size header
            int sizePos = out.writerIndex();
            out.writerIndex(out.writerIndex() + 4);
            try (StreamOutput output = streamService.output(out)) {
                Streamable message = ((StreamableTransportFrame) frame).message();
                output.writeClass(message.getClass());
                output.writeStreamable(message);
            }
            int size = out.writerIndex() - sizePos - 4;
            out.setInt(sizePos, size);
        } else {
            // ping request
            out.writeInt(0);
        }
    }
}
