package org.mitallast.queue.transport.transport;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.mitallast.queue.transport.TransportController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class TransportServerHandler extends SimpleChannelInboundHandler<TransportFrame> {

    private final static Logger logger = LoggerFactory.getLogger(TransportServerHandler.class);
    private final TransportController transportController;

    public TransportServerHandler(TransportController transportController) {
        this.transportController = transportController;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TransportFrame request) throws Exception {
        TransportFrame response = new TransportFrame(
            request.getVersion(),
            request.getRequest(),
            request.getSize(),
            request.getContent().retain()
        );

        ctx.write(response, ctx.voidPromise());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }
}
