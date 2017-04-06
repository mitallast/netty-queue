package org.mitallast.queue.transport.netty;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportServer;

public class NettyTransportServer extends NettyServer implements TransportServer {

    private final DiscoveryNode discoveryNode;
    private final TransportController transportController;
    private final StreamService streamService;

    @Inject
    public NettyTransportServer(
        Config config,
        TransportController transportController,
        StreamService streamService
    ) {
        super(config.getConfig("transport"));
        this.transportController = transportController;
        this.streamService = streamService;
        this.discoveryNode = new DiscoveryNode(host, port);
    }

    @Override
    public DiscoveryNode localNode() {
        return discoveryNode;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new TransportServerInitializer();
    }

    private class TransportServerInitializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new StreamableDecoder(streamService));
            pipeline.addLast(new StreamableEncoder(streamService));
            pipeline.addLast(new TransportServerHandler());
        }
    }

    @ChannelHandler.Sharable
    private class TransportServerHandler extends SimpleChannelInboundHandler<Streamable> {

        public TransportServerHandler() {
            super(true);
        }

        @Override
        public boolean isSharable() {
            return true;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Streamable message) {
            transportController.dispatch(message);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("unexpected channel error, close channel", cause);
            ctx.close();
        }
    }
}
