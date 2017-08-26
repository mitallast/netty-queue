package org.mitallast.queue.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class CodecEncoder extends MessageToByteEncoder<Message> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Message message, ByteBuf out) throws Exception {
        int sizePos = out.writerIndex();
        out.writerIndex(out.writerIndex() + 4);
        ByteBufOutputStream stream = new ByteBufOutputStream(out);
        Codec.anyCodec().write(stream, message);
        int size = out.writerIndex() - sizePos - 4;
        out.setInt(sizePos, size);
    }
}
