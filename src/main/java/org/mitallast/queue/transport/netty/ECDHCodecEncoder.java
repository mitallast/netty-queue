package org.mitallast.queue.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.ecdh.ECDHFlow;
import org.mitallast.queue.ecdh.Encrypted;
import org.mitallast.queue.ecdh.RequestStart;
import org.mitallast.queue.ecdh.ResponseStart;

import java.util.List;

public class ECDHCodecEncoder extends MessageToMessageEncoder<Message> {
    private final static Logger logger = LogManager.getLogger();
    private final static AttributeKey<ECDHFlow> ECDHKey = AttributeKey.valueOf("ECDH");

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        if (msg instanceof RequestStart) {
            out.add(msg);
        } else if (msg instanceof ResponseStart) {
            out.add(msg);
        } else if (msg instanceof Encrypted) {
            out.add(msg);
        } else {
            ECDHFlow ecdhFlow = ctx.channel().attr(ECDHKey).get();
            if (ecdhFlow == null) {
                throw new IllegalStateException("no ecdh");
            }
            if (!ecdhFlow.isAgreement()) {
                ecdhFlow.agreementFuture().whenComplete((x, e) -> ctx.writeAndFlush(msg));
            } else {
                ByteBuf buffer = ctx.alloc().buffer();
                try {
                    ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer);
                    Codec.anyCodec().write(outputStream, msg);
                    outputStream.close();
                    int len = buffer.readableBytes();
                    byte[] data = new byte[len];
                    buffer.readBytes(data);
                    Encrypted encrypted = ecdhFlow.encrypt(data);
                    out.add(encrypted);
                } catch (Exception e) {
                    logger.error("error encrypt", e);
                    throw e;
                } finally {
                    buffer.release();
                }
            }
        }
    }
}
