package org.mitallast.queue.transport.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class TransportFrameEncoder extends MessageToByteEncoder<TransportFrame> {

    @Override
    protected void encode(ChannelHandlerContext ctx, TransportFrame msg, ByteBuf out) throws Exception {
        out.writeByte('E');
        out.writeByte('Q');
        out.writeInt(msg.getVersion().id);
        out.writeLong(msg.getRequest());
        out.writeInt(msg.getSize());
        msg.getContent().readBytes(out, msg.getSize());
    }
}
