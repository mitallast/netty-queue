package org.mitallast.queue.transport.netty

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

class CodecDecoder : ByteToMessageDecoder() {
    private val logger = LogManager.getLogger()

    @Throws(Exception::class)
    public override fun decode(ctx: ChannelHandlerContext, buffer: ByteBuf, out: MutableList<Any>) {
        if (buffer.readableBytes() < Integer.BYTES) {
            return
        }

        val readerIndex = buffer.readerIndex()
        val size = buffer.getInt(readerIndex)

        if (buffer.readableBytes() < size + Integer.BYTES) {
            return
        }

        buffer.skipBytes(Integer.BYTES)
        val start = buffer.readerIndex()
        val message: Message
        val stream = ByteBufInputStream(buffer)
        message = Codec.anyCodec<Message>().read(stream)
        val readSize = buffer.readerIndex() - start
        if (readSize < size) {
            logger.warn("error reading message, expected {} read {}, skip bytes", size, readSize)
            buffer.readerIndex(buffer.readerIndex() + size - readSize)
        } else if (readSize > size) {
            logger.warn("error reading message, expected {} read {}", size, readSize)
        }
        out.add(message)
    }
}
