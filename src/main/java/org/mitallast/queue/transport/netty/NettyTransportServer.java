package org.mitallast.queue.transport.netty;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.*;
import io.netty.util.AttributeKey;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.netty.NettyProvider;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.ecdh.ECDHFlow;
import org.mitallast.queue.ecdh.RequestStart;
import org.mitallast.queue.ecdh.ResponseStart;
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
            ECDHFlow ecdh = new ECDHFlow();
            ctx.channel().attr(ECDHKey).set(ecdh);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message message) throws Exception {
            if (message instanceof RequestStart) {
                logger.info("received ecdh request start");
                ECDHFlow ecdh = ctx.channel().attr(ECDHKey).get();
                RequestStart start = (RequestStart) message;
                ecdh.keyAgreement(start.publicKey());
                logger.info("send ecdh response start");
                ctx.writeAndFlush(new ResponseStart(ecdh.publicKey()));
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
