package org.mitallast.queue.transport.netty

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.security.ECDHEncrypted
import org.mitallast.queue.security.ECDHFlow
import java.io.ByteArrayInputStream
import java.io.DataInputStream

class ECDHCodecDecoder : MessageToMessageDecoder<Message>() {
    override fun decode(ctx: ChannelHandlerContext, msg: Message, out: MutableList<Any>) {
        if (msg is ECDHEncrypted) {
            val ecdhFlow = ctx.channel().attr(ECDHFlow.key).get()
            val decrypted = ecdhFlow.decrypt(msg)
            val input = DataInputStream(ByteArrayInputStream(decrypted))
            val decoded = Codec.anyCodec<Message>().read(input)
            out.add(decoded)
        } else {
            out.add(msg)
        }
    }
}
