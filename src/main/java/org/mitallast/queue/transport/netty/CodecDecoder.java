package org.mitallast.queue.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

import java.io.DataInput;
import java.util.List;

public class CodecDecoder extends ByteToMessageDecoder {
    private final static Logger logger = LogManager.getLogger();

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        if (buffer.readableBytes() < Integer.BYTES) {
            return;
        }

        int readerIndex = buffer.readerIndex();
        final int size = buffer.getInt(readerIndex);

        if (buffer.readableBytes() < size + Integer.BYTES) {
            return;
        }

        buffer.skipBytes(Integer.BYTES);
        int start = buffer.readerIndex();
        final Message message;
        DataInput stream = new ByteBufInputStream(buffer);
        message = Codec.anyCodec().read(stream);
        int readSize = buffer.readerIndex() - start;
        if (readSize < size) {
            logger.warn("error reading message, expected {} read {}, skip bytes", size, readSize);
            buffer.readerIndex(buffer.readerIndex() + size - readSize);
        } else if (readSize > size) {
            logger.warn("error reading message, expected {} read {}", size, readSize);
        }
        out.add(message);
    }
}
