package org.mitallast.queue.transport.netty.codec;

import com.google.protobuf.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.mitallast.queue.common.proto.ProtoService;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * uint32NoTag index
 * uint32NoTag size
 * byte[size] protobuf delimited
 */
public class TransportFrameDecoder extends ByteToMessageDecoder {

    private final ProtoService protoService;

    public TransportFrameDecoder(ProtoService protoService) {
        this.protoService = protoService;
    }

    @SuppressWarnings("unchecked")
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

            Parser<? extends Message> parser = protoService.parser(index);

            ByteBuffer byteBuffer = buffer.nioBuffer(buffer.readerIndex(), size);
            CodedInputStream coded = CodedInputStream.newInstance(byteBuffer);

            int limit = coded.pushLimit(size);
            try {
                Message message = parser.parseFrom(coded, ExtensionRegistryLite.getEmptyRegistry());
                out.add(message);
            } finally {
                coded.popLimit(limit);
            }
            buffer.readerIndex(header + 8 + size);
        }
    }
}
