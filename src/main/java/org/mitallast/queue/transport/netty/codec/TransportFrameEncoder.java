package org.mitallast.queue.transport.netty.codec;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.mitallast.queue.common.proto.ProtoService;

/**
 * int32 index
 * int32 size
 * byte[size] protobuf delimited
 */
public class TransportFrameEncoder extends MessageToByteEncoder<Message> {

    private final ProtoService protoService;

    public TransportFrameEncoder(ProtoService protoService) {
        this.protoService = protoService;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Message message, ByteBuf out) throws Exception {
        int index = protoService.index(message.getDescriptorForType());
        int header = out.writerIndex();
        out.writeInt(index);
        out.writeInt(0);
        ByteBufOutputStream stream = new ByteBufOutputStream(out);
        message.writeTo(stream);
        int size = out.writerIndex() - 8 - header;
        out.setInt(header + 4, size);
    }
}
