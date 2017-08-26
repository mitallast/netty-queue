package org.mitallast.queue.transport.netty;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.*;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.netty.NettyProvider;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportServer;

public class NettyTransportServer extends NettyServer implements TransportServer {

    private final DiscoveryNode discoveryNode;
    private final TransportController transportController;

    @Inject
    public NettyTransportServer(
        Config config,
        NettyProvider provider,
        TransportController transportController
    ) {
        super(config, provider,
            config.getString("transport.host"),
            config.getInt("transport.port")
        );
        this.transportController = transportController;
        this.discoveryNode = new DiscoveryNode(host, port);
    }

    @Override
    public DiscoveryNode localNode() {
        return discoveryNode;
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new TransportServerInitializer();
    }

    private class TransportServerInitializer extends ChannelInitializer {
        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new CodecDecoder());
            pipeline.addLast(new CodecEncoder());
            pipeline.addLast(new TransportServerHandler());
        }
    }

    @ChannelHandler.Sharable
    private class TransportServerHandler extends SimpleChannelInboundHandler<Message> {

        public TransportServerHandler() {
            super(true);
        }

        @Override
        public boolean isSharable() {
            return true;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message message) {
            transportController.dispatch(message);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("unexpected channel error, close channel", cause);
            ctx.close();
        }
    }
}
