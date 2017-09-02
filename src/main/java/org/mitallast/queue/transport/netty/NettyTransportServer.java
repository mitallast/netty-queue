package org.mitallast.queue.transport.netty;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.*;
import io.netty.util.AttributeKey;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.netty.NettyProvider;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.security.ECDHFlow;
import org.mitallast.queue.security.ECDHRequest;
import org.mitallast.queue.security.SecurityService;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportServer;

public class NettyTransportServer extends NettyServer implements TransportServer {

    private final DiscoveryNode discoveryNode;
    private final TransportController transportController;
    private final SecurityService securityService;

    @Inject
    public NettyTransportServer(
            Config config,
            NettyProvider provider,
            TransportController transportController,
            SecurityService securityService
    ) {
        super(config, provider,
                config.getString("transport.host"),
                config.getInt("transport.port")
        );
        this.transportController = transportController;
        this.securityService = securityService;
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
            pipeline.addLast(new ECDHCodecEncoder());
            pipeline.addLast(new ECDHCodecDecoder());
            pipeline.addLast(new TransportServerHandler());
        }
    }

    @ChannelHandler.Sharable
    private class TransportServerHandler extends SimpleChannelInboundHandler<Message> {
        private final AttributeKey<ECDHFlow> ECDHKey = AttributeKey.valueOf("ECDH");

        public TransportServerHandler() {
            super(true);
        }

        @Override
        public boolean isSharable() {
            return true;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            ctx.channel().attr(ECDHKey).set(securityService.ecdh());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message message) throws Exception {
            if (message instanceof ECDHRequest) {
                logger.info("received ecdh request start");
                ECDHFlow ecdh = ctx.channel().attr(ECDHKey).get();
                ECDHRequest start = (ECDHRequest) message;
                ecdh.keyAgreement(start);
                logger.info("send ecdh response start");
                ctx.writeAndFlush(ecdh.responseStart());
            } else {
                transportController.dispatch(message);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("unexpected channel error, close channel", cause);
            ctx.close();
        }
    }
}
