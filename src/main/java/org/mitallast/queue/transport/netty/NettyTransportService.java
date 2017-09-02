package org.mitallast.queue.transport.netty;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.*;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.common.netty.NettyClientBootstrap;
import org.mitallast.queue.common.netty.NettyProvider;
import org.mitallast.queue.security.ECDHFlow;
import org.mitallast.queue.security.ECDHResponse;
import org.mitallast.queue.security.SecurityService;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class NettyTransportService extends NettyClientBootstrap implements TransportService {
    private final ReentrantLock connectionLock = new ReentrantLock();
    private final int maxConnections;
    private final TransportController transportController;
    private final SecurityService securityService;
    private volatile Map<DiscoveryNode, NodeChannel> connectedNodes = HashMap.empty();

    @Inject
    public NettyTransportService(
            Config config,
            NettyProvider provider,
            TransportController transportController,
            SecurityService securityService
    ) {
        super(config, provider);
        this.transportController = transportController;
        this.securityService = securityService;
        maxConnections = config.getInt("transport.max_connections");
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new CodecDecoder());
                pipeline.addLast(new CodecEncoder());
                pipeline.addLast(new ECDHCodecEncoder());
                pipeline.addLast(new ECDHCodecDecoder());
                pipeline.addLast(new SimpleChannelInboundHandler<Message>(false) {

                    @Override
                    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
                        logger.info("start ecdh");
                        ctx.channel().attr(ECDHFlow.key).set(securityService.ecdh());
                    }

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        logger.info("send ecdh request start");
                        ECDHFlow ecdh = ctx.channel().attr(ECDHFlow.key).get();
                        ctx.writeAndFlush(ecdh.requestStart());
                        super.channelActive(ctx);
                    }

                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Message message) throws Exception {
                        ECDHFlow ecdh = ctx.channel().attr(ECDHFlow.key).get();
                        if (message instanceof ECDHResponse) {
                            logger.info("received response ecdh start");
                            ECDHResponse start = (ECDHResponse) message;
                            ecdh.keyAgreement(start);
                        } else {
                            transportController.dispatch(message);
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        logger.error("unexpected exception {}", ctx, cause);
                        ctx.close();
                    }
                });
            }
        };
    }

    @Override
    protected void doStop() {
        connectedNodes.keySet().forEach(this::disconnectFromNode);
        super.doStop();
    }

    @Override
    public void connectToNode(DiscoveryNode node) {
        checkIsStarted();
        Preconditions.checkNotNull(node);
        if (connectedNodes.getOrElse(node, null) != null) {
            return;
        }
        connectionLock.lock();
        try {
            if (connectedNodes.getOrElse(node, null) != null) {
                return;
            }
            NodeChannel nodeChannel = new NodeChannel(node);
            connectedNodes = connectedNodes.put(node, nodeChannel);
            nodeChannel.open();
            logger.info("connected to node {}", node);
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        Preconditions.checkNotNull(node);
        connectionLock.lock();
        try {
            NodeChannel nodeChannel = connectedNodes.getOrElse(node, null);
            if (nodeChannel == null) {
                return;
            }
            logger.info("disconnect from node {}", node);
            nodeChannel.close();
            connectedNodes = connectedNodes.remove(node);
        } finally {
            connectionLock.unlock();
        }
    }

    private TransportChannel channel(DiscoveryNode node) {
        Preconditions.checkNotNull(node);
        NodeChannel nodeChannel = connectedNodes.getOrElse(node, null);
        if (nodeChannel == null) {
            throw new IllegalArgumentException("Not connected to node: " + node);
        }
        return nodeChannel;
    }

    @Override
    public void send(DiscoveryNode node, Message message) {
        try {
            connectToNode(node);
            channel(node).send(message);
        } catch (Exception e) {
            logger.error("error send message", e);
        }
    }

    private class NodeChannel implements TransportChannel, Closeable {
        private final DiscoveryNode node;
        private final AtomicLong channelCounter = new AtomicLong();
        private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Channel[] channels;

        private NodeChannel(DiscoveryNode node) {
            this.node = node;
            this.channels = new Channel[maxConnections];
        }

        private synchronized void open() {
            logger.info("connect to {}", node);
            ChannelFuture[] channelFutures = new ChannelFuture[maxConnections];
            for (int i = 0; i < maxConnections; i++) {
                channelFutures[i] = connect(node);
            }
            logger.debug("await channel open {}", node);
            for (int i = 0; i < maxConnections; i++) {
                try {
                    Channel channel = channelFutures[i]
                            .awaitUninterruptibly()
                            .channel();
                    channels[i] = channel;
                } catch (Throwable e) {
                    logger.error("error connect to {}", node, e);
                    if (reconnectScheduled.compareAndSet(false, true)) {
                        provider.child().execute(this::reconnect);
                    }
                }
            }
        }

        private synchronized void reconnect() {
            if (closed.get()) {
                return;
            }
            logger.warn("reconnect to {}", node);
            for (int i = 0; i < channels.length; i++) {
                if (channels[i] == null || !channels[i].isOpen()) {
                    try {
                        Channel channel = connect(node)
                                .awaitUninterruptibly()
                                .channel();
                        channels[i] = channel;
                    } catch (Throwable e) {
                        logger.error("error reconnect to {}", node, e);
                    }
                }
            }
            reconnectScheduled.set(false);
        }

        @Override
        public void send(Message message) {
            Channel channel = channel();
            ECDHFlow ecdh = channel.attr(ECDHFlow.key).get();
            if (ecdh.isAgreement()) {
                channel.writeAndFlush(message, channel.voidPromise());
            } else {
                ecdh.agreementFuture().whenComplete((a, e) ->
                        channel.writeAndFlush(message, channel.voidPromise()));
            }
        }

        @Override
        public synchronized void close() {
            closed.set(true);
            for (Channel channel : channels) {
                channel.close();
            }
        }

        private Channel channel() {
            int index = (int) channelCounter.get() % channels.length;
            channelCounter.set(index + 1);
            int loopIndex = index;
            do {
                if (channels[index] != null && channels[index].isOpen()) {
                    return channels[index];
                } else if (reconnectScheduled.compareAndSet(false, true)) {
                    provider.child().execute(this::reconnect);
                }
                index = (index + 1) % channels.length;
            } while (index != loopIndex);
            throw new RuntimeException("error connect to " + node);
        }
    }
}
