package org.mitallast.queue.transport.netty

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufOutputStream
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

class CodecEncoder : MessageToByteEncoder<Message>() {

    @Throws(Exception::class)
    public override fun encode(ctx: ChannelHandlerContext, message: Message, out: ByteBuf) {
        val sizePos = out.writerIndex()
        out.writerIndex(out.writerIndex() + 4)
        val stream = ByteBufOutputStream(out)
        Codec.anyCodec<Message>().write(stream, message)
        val size = out.writerIndex() - sizePos - 4
        out.setInt(sizePos, size)
    }
}
