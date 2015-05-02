package org.mitallast.queue.transport.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.mitallast.queue.Version;
import org.mitallast.queue.transport.TransportFrame;

import java.io.IOException;
import java.util.List;

public class TransportFrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        while (true) {
            if (buffer.readableBytes() < TransportFrame.HEADER_SIZE) {
                break;
            }
            int readerIndex = buffer.readerIndex();
            if (buffer.getByte(readerIndex) != 'E' && buffer.getByte(readerIndex + 1) != 'Q') {
                throw new IOException("Invalid header");
            }

            final Version version = Version.fromId(buffer.getInt(readerIndex + 2));
            final long request = buffer.getLong(readerIndex + 2 + 4);
            final int size = buffer.getInt(readerIndex + 2 + 4 + 8);

            final ByteBuf content;
            if (size <= 0) {
                // ping request
                content = null;
                buffer.skipBytes(TransportFrame.HEADER_SIZE);
            } else {
                // standard request
                if (buffer.readableBytes() < size + TransportFrame.HEADER_SIZE) {
                    break;
                }
                buffer.skipBytes(TransportFrame.HEADER_SIZE);
                content = buffer.readSlice(size).retain();
            }


            out.add(TransportFrame.of(version, request, size, content));
        }
    }
}
