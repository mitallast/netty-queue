package org.mitallast.queue.transport.netty;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.security.ECDHEncrypted;
import org.mitallast.queue.security.ECDHFlow;

import java.util.List;

public class ECDHCodecDecoder extends MessageToMessageDecoder<Message> {
    @Override
    protected void decode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        if (msg instanceof ECDHEncrypted) {
            ECDHFlow ecdhFlow = ctx.channel().attr(ECDHFlow.key).get();
            byte[] decrypted = ecdhFlow.decrypt((ECDHEncrypted) msg);
            ByteArrayDataInput input = ByteStreams.newDataInput(decrypted);
            Message decoded = Codec.anyCodec().read(input);
            out.add(decoded);
        } else {
            out.add(msg);
        }
    }
}
