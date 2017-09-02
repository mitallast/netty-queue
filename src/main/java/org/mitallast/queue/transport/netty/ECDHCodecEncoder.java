package org.mitallast.queue.transport.netty;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.security.ECDHEncrypted;
import org.mitallast.queue.security.ECDHFlow;
import org.mitallast.queue.security.ECDHRequest;
import org.mitallast.queue.security.ECDHResponse;

import java.util.List;

public class ECDHCodecEncoder extends MessageToMessageEncoder<Message> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        if (msg instanceof ECDHRequest) {
            out.add(msg);
        } else if (msg instanceof ECDHResponse) {
            out.add(msg);
        } else if (msg instanceof ECDHEncrypted) {
            out.add(msg);
        } else {
            ECDHFlow ecdhFlow = ctx.channel().attr(ECDHFlow.key).get();
            ByteArrayDataOutput output = ByteStreams.newDataOutput();
            Codec.Companion.anyCodec().write(output, msg);
            byte[] data = output.toByteArray();
            ECDHEncrypted encrypted = ecdhFlow.encrypt(data);
            out.add(encrypted);
        }
    }
}
