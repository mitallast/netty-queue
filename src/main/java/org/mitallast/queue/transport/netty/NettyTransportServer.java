package org.mitallast.queue.transport.netty;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.transport.*;
import org.mitallast.queue.transport.netty.codec.*;

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
        super(config.getConfig("transport"), TransportServer.class);
        this.transportController = transportController;
        this.streamService = streamService;
        this.discoveryNode = new DiscoveryNode(HostAndPort.fromParts(host, port));
    }

    @Override
    public HostAndPort localAddress() {
        return discoveryNode.address();
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
            pipeline.addLast(new TransportFrameDecoder(streamService));
            pipeline.addLast(new TransportFrameEncoder(streamService));
            pipeline.addLast(new TransportServerHandler());
        }
    }

    @ChannelHandler.Sharable
    private class TransportServerHandler extends SimpleChannelInboundHandler<TransportFrame> {

        public TransportServerHandler() {
            super(false);
        }

        @Override
        public boolean isSharable() {
            return true;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            ctx.channel().attr(NettyTransportChannel.channelAttr).set(new NettyTransportChannel(ctx));
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, TransportFrame request) {
            NettyTransportChannel transportChannel = ctx.channel().attr(NettyTransportChannel.channelAttr).get();
            transportController.dispatch(transportChannel, request);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("unexpected channel error, close channel", cause);
            ctx.close();
        }
    }
}
