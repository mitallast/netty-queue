package org.mitallast.queue.transport.netty

import com.google.common.io.ByteStreams
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.security.ECDHEncrypted
import org.mitallast.queue.security.ECDHFlow
import org.mitallast.queue.security.ECDHRequest
import org.mitallast.queue.security.ECDHResponse

class ECDHCodecEncoder : MessageToMessageEncoder<Message>() {
    override fun encode(ctx: ChannelHandlerContext, msg: Message, out: MutableList<Any>) {
        when (msg) {
            is ECDHRequest -> out.add(msg)
            is ECDHResponse -> out.add(msg)
            is ECDHEncrypted -> out.add(msg)
            else -> {
                val ecdhFlow = ctx.channel().attr(ECDHFlow.key).get()
                val output = ByteStreams.newDataOutput()
                Codec.anyCodec<Message>().write(output, msg)
                val data = output.toByteArray()
                val encrypted = ecdhFlow.encrypt(data)
                out.add(encrypted)
            }
        }
    }
}
