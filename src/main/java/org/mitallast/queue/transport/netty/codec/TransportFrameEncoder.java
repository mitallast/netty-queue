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
        out.writeShort(frame.version().id);
        out.writeByte(frame.type().ordinal());

        // ping does not have body

        if (frame.type() == TransportFrameType.MESSAGE) {
            MessageTransportFrame request = (MessageTransportFrame) frame;
            int sizePos = out.writerIndex();
            out.writerIndex(out.writerIndex() + 4);
            try (StreamOutput output = streamService.output(out)) {
                Streamable message = request.message();
                output.writeClass(message.getClass());
                output.writeStreamable(message);
            }
            int size = out.writerIndex() - sizePos - 4;
            out.setInt(sizePos, size);
        }
    }
}
