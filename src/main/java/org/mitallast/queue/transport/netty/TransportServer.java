package org.mitallast.queue.transport.netty;

import com.google.inject.Inject;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportFrame;
import org.mitallast.queue.transport.TransportModule;
import org.mitallast.queue.transport.netty.codec.TransportFrameDecoder;
import org.mitallast.queue.transport.netty.codec.TransportFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportServer extends NettyServer {

    private final TransportController transportController;

    @Inject
    public TransportServer(Settings settings, TransportController transportController) {
        super(settings, TransportServer.class, TransportModule.class);
        this.transportController = transportController;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new TransportServerInitializer(new TransportServerHandler(transportController));
    }

    protected int defaultPort() {
        return 10080;
    }

    private static class TransportServerInitializer extends ChannelInitializer<SocketChannel> {

        private final TransportServerHandler transportServerHandler;

        public TransportServerInitializer(TransportServerHandler transportServerHandler) {

            this.transportServerHandler = transportServerHandler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new TransportFrameDecoder());
            pipeline.addLast(new TransportFrameEncoder());
            pipeline.addLast(transportServerHandler);
        }
    }

    @ChannelHandler.Sharable
    private static class TransportServerHandler extends SimpleChannelInboundHandler<TransportFrame> {

        private final static Logger logger = LoggerFactory.getLogger(TransportServerHandler.class);
        private final TransportController transportController;

        public TransportServerHandler(TransportController transportController) {
            this.transportController = transportController;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, TransportFrame request) throws Exception {
            if (request.isPing()) {
                ctx.write(TransportFrame.of(request), ctx.voidPromise());
                return;
            }
            TransportChannel channel = new NettyTransportChannel(ctx);
            transportController.dispatchRequest(channel, request);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("unexpected channel error, close channel", cause);
            ctx.close();
        }
    }
}
