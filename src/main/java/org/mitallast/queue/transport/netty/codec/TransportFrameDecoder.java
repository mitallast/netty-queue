package org.mitallast.queue.transport.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.mitallast.queue.common.proto.ProtoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * uint32NoTag index
 * uint32NoTag size
 * byte[size] protobuf delimited
 */
public class TransportFrameDecoder extends ByteToMessageDecoder {
    private final static Logger logger = LoggerFactory.getLogger(TransportFrameDecoder.class);

    private final ProtoService protoService;

    public TransportFrameDecoder(ProtoService protoService) {
        this.protoService = protoService;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        while (true) {
            if (buffer.readableBytes() < 8) {
                break;
            }
            int header = buffer.readerIndex();
            int index = buffer.getInt(header);
            int size = buffer.getInt(header + 4);
            if (buffer.readableBytes() < size + 8) {
                break;
            }
            buffer.skipBytes(8);
            ByteBufInputStream limited = new ByteBufInputStream(buffer, size);
            Object message = protoService.parser(index).parseFrom(limited);

            int skip = buffer.readerIndex() - (header + 8 + size);
            if (skip > 0) {
                logger.warn("unexpected available bytes: {}", skip);
                buffer.skipBytes(skip);
            }
            out.add(message);
        }
    }
}
