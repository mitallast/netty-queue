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

import java.util.List;

public class TransportFrameDecoder extends ByteToMessageDecoder {
    private final static Logger logger = LoggerFactory.getLogger(TransportFrameDecoder.class);
    private final static int HEADER_SIZE = 2 + 1;
    private final static int MESSAGE_HEADER_SIZE = HEADER_SIZE + Integer.BYTES;

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

            final Version version = Version.fromId(buffer.getShort(readerIndex));
            int type = buffer.getByte(readerIndex + 2);

            if (type == TransportFrameType.PING.ordinal()) {
                buffer.readerIndex(readerIndex + HEADER_SIZE);
                if (version.equals(Version.CURRENT)) {
                    out.add(PingTransportFrame.CURRENT);
                }else {
                    out.add(new PingTransportFrame(version));
                }
            } else if (type == TransportFrameType.MESSAGE.ordinal()) {
                if (buffer.readableBytes() < MESSAGE_HEADER_SIZE) {
                    break;
                }

                final int size = buffer.getInt(readerIndex + HEADER_SIZE);

                if (buffer.readableBytes() < size + MESSAGE_HEADER_SIZE) {
                    break;
                }
                buffer.readerIndex(buffer.readerIndex() + MESSAGE_HEADER_SIZE);
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
                out.add(new MessageTransportFrame(version, message));
            }
        }
    }
}
