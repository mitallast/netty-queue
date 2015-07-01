package org.mitallast.queue.transport.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class TransportFrameDecoder extends ByteToMessageDecoder {
    private final static Logger logger = LoggerFactory.getLogger(TransportFrameDecoder.class);
    private final static int HEADER_SIZE = 2 + 4 + 8 + 4;

    private final StreamService streamService;

    public TransportFrameDecoder(StreamService streamService) {
        this.streamService = streamService;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        while (true) {
            if (buffer.readableBytes() < HEADER_SIZE) {
                break;
            }
            int readerIndex = buffer.readerIndex();
            if (buffer.getByte(readerIndex) != 'E' && buffer.getByte(readerIndex + 1) != 'Q') {
                throw new IOException("Invalid header");
            }

            final Version version = Version.fromId(buffer.getInt(readerIndex + 2));
            final long request = buffer.getLong(readerIndex + 2 + 4);
            final int size = buffer.getInt(readerIndex + 2 + 4 + 8);

            if (size <= 0) {
                // ping request
                buffer.readerIndex(buffer.readerIndex() + HEADER_SIZE);
                out.add(TransportFrame.of(version, request));
            } else {
                // standard request
                if (buffer.readableBytes() < size + HEADER_SIZE) {
                    break;
                }
                buffer.readerIndex(buffer.readerIndex() + HEADER_SIZE);
                int start = buffer.readerIndex();
                final Streamable message;
                try (StreamInput input = streamService.input(buffer)) {
                    message = input.readStreamable();
                }
                int readSize = buffer.readerIndex() - start;
                if (readSize < size) {
                    logger.warn("error reading message, expected {} read {}, skip bytes", size, readSize);
                    buffer.readerIndex(buffer.readerIndex() + size - readSize);
                } else if (readSize > size) {
                    logger.warn("error reading message, expected {} read {}", size, readSize);
                }


                out.add(StreamableTransportFrame.of(version, request, message));
            }
        }
    }
}
