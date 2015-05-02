package org.mitallast.queue.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportFrame;

public class NettyTransportChannel implements TransportChannel {

    private final ChannelHandlerContext ctx;

    public NettyTransportChannel(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void send(TransportFrame response) {
        ctx.write(response, ctx.voidPromise());
    }

    @Override
    public ByteBuf ioBuffer() {
        return ctx.alloc().ioBuffer();
    }

    @Override
    public ByteBuf heapBuffer() {
        return ctx.alloc().heapBuffer();
    }
}
