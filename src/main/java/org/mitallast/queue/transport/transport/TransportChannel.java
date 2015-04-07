package org.mitallast.queue.transport.transport;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

public class TransportChannel {
    private final ChannelHandlerContext ctx;
    private final TransportFrame request;

    public TransportChannel(ChannelHandlerContext ctx, TransportFrame request) {
        this.ctx = ctx;
        this.request = request;
    }

    public void send(TransportFrame response) throws IOException {
        ctx.write(response, ctx.voidPromise());
    }

    public ChannelHandlerContext ctx() {
        return ctx;
    }

    public TransportFrame request() {
        return request;
    }
}
