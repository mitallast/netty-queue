package org.mitallast.queue.transport.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.mitallast.queue.transport.TransportFrame;

public class TransportFrameEncoder extends MessageToByteEncoder<TransportFrame> {

    @Override
    protected void encode(ChannelHandlerContext ctx, TransportFrame msg, ByteBuf out) throws Exception {
        out.writeByte('E');
        out.writeByte('Q');
        out.writeInt(msg.getVersion().id);
        out.writeLong(msg.getRequest());
        out.writeInt(msg.getSize());
        if (msg.getSize() > 0) {
            out.ensureWritable(msg.getSize());
            msg.getContent().readBytes(out, msg.getSize());
        }
    }
}
