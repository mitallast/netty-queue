package org.mitallast.queue.transport.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.netty.codec.TransportFrame;

public class NettyTransportChannel implements TransportChannel {
    public final static AttributeKey<NettyTransportChannel> channelAttr = AttributeKey.valueOf("transport");
    private final ChannelHandlerContext ctx;

    public NettyTransportChannel(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void send(TransportFrame frame) {
        ctx.writeAndFlush(frame, ctx.voidPromise());
    }

    @Override
    public void close() {
        ctx.close();
    }
}
