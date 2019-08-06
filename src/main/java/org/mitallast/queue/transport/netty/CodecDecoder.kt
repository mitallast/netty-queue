package org.mitallast.queue.transport.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.codec.UnsafeByteBufInputStream
import org.mitallast.queue.common.logging.LoggingService

class CodecDecoder(logging: LoggingService) : ByteToMessageDecoder() {
    private val logger = logging.logger()

    @Throws(Exception::class)
    public override fun decode(ctx: ChannelHandlerContext?, buffer: ByteBuf, out: MutableList<Any>) {
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
        val stream = UnsafeByteBufInputStream(buffer)
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
