package org.mitallast.queue.transport.netty;

import com.google.inject.Inject;
import com.google.protobuf.Message;
import com.typesafe.config.Config;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.proto.ProtoService;
import org.mitallast.queue.proto.raft.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportServer;
import org.mitallast.queue.transport.netty.codec.TransportFrameDecoder;
import org.mitallast.queue.transport.netty.codec.TransportFrameEncoder;


public class NettyTransportServer extends NettyServer implements TransportServer {

    private final DiscoveryNode discoveryNode;
    private final TransportController transportController;
    private final ProtoService protoService;

    @Inject
    public NettyTransportServer(
            Config config,
            TransportController transportController,
            ProtoService protoService
    ) {
        super(config.getConfig("transport"), TransportServer.class);
        this.transportController = transportController;
        this.protoService = protoService;
        this.discoveryNode = DiscoveryNode.newBuilder().setHost(host).setPort(port).build();
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
            pipeline.addLast(new TransportFrameDecoder(protoService));
            pipeline.addLast(new TransportFrameEncoder(protoService));
            pipeline.addLast(new TransportServerHandler());
        }
    }

    @ChannelHandler.Sharable
    private class TransportServerHandler extends SimpleChannelInboundHandler<Message> {

        TransportServerHandler() {
            super(false);
        }

        @Override
        public boolean isSharable() {
            return true;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message request) {
            transportController.dispatch(request);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("unexpected channel error, close channel", cause);
            ctx.close();
        }
    }
}
