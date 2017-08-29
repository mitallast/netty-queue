package org.mitallast.queue.transport.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.AttributeKey;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.ecdh.ECDHFlow;
import org.mitallast.queue.ecdh.Encrypted;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.List;

public class ECDHCodecDecoder extends MessageToMessageDecoder<Message> {
    private final static AttributeKey<ECDHFlow> ECDHKey = AttributeKey.valueOf("ECDH");

    @Override
    protected void decode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        if (msg instanceof Encrypted) {
            ECDHFlow ecdhFlow = ctx.channel().attr(ECDHKey).get();
            if (ecdhFlow == null) {
                throw new IllegalStateException("no ecdh");
            }
            byte[] decrypted = ecdhFlow.decrypt((Encrypted) msg);
            ByteArrayInputStream stream = new ByteArrayInputStream(decrypted);
            Message decoded = Codec.anyCodec().read(new DataInputStream(stream));
            out.add(decoded);
        } else {
            out.add(msg);
        }
    }
}
