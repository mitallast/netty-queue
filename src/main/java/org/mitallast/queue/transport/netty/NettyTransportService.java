package org.mitallast.queue.transport.netty;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.*;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.netty.NettyClientBootstrap;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportChannel;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;
import org.mitallast.queue.transport.netty.codec.TransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrameDecoder;
import org.mitallast.queue.transport.netty.codec.TransportFrameEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class NettyTransportService extends NettyClientBootstrap implements TransportService {
    private final ReentrantLock connectionLock;
    private final int maxConnections;
    private final TransportController transportController;
    private final StreamService streamService;
    private final DefaultEventExecutor executor;
    private volatile ImmutableMap<DiscoveryNode, NodeChannel> connectedNodes;

    @Inject
    public NettyTransportService(Config config, TransportController transportController, StreamService streamService) {
        super(config.getConfig("transport"), TransportService.class);
        this.transportController = transportController;
        this.streamService = streamService;
        maxConnections = this.config.getInt("max_connections");
        connectedNodes = ImmutableMap.of();
        connectionLock = new ReentrantLock();
        executor = new DefaultEventExecutor(new DefaultThreadFactory("connect"));
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new TransportFrameDecoder(streamService));
                pipeline.addLast(new TransportFrameEncoder(streamService));
                pipeline.addLast(new SimpleChannelInboundHandler<TransportFrame>(false) {

                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, TransportFrame frame) throws Exception {
                        transportController.dispatch(frame);
                    }

                    @Override
                    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
                        ctx.flush();
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
    protected void doStop() throws IOException {
        ImmutableMap<DiscoveryNode, NodeChannel> connectedNodes = this.connectedNodes;
        connectedNodes.keySet().forEach(this::disconnectFromNode);
        executor.shutdownGracefully();
        super.doStop();
    }

    @Override
    public void connectToNode(DiscoveryNode node) {
        checkIsStarted();
        Preconditions.checkNotNull(node);
        if (connectedNodes.get(node) != null) {
            return;
        }
        connectionLock.lock();
        try {
            if (connectedNodes.get(node) != null) {
                return;
            }
            NodeChannel nodeChannel = new NodeChannel(node);
            connectedNodes = Immutable.compose(connectedNodes, node, nodeChannel);
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
            NodeChannel nodeChannel = connectedNodes.get(node);
            if (nodeChannel == null) {
                return;
            }
            logger.info("disconnect from node {}", node);
            nodeChannel.close();
            connectedNodes = Immutable.subtract(connectedNodes, node);
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public TransportChannel channel(DiscoveryNode node) {
        Preconditions.checkNotNull(node);
        NodeChannel nodeChannel = connectedNodes.get(node);
        if (nodeChannel == null) {
            throw new IllegalArgumentException("Not connected to node: " + node);
        }
        return nodeChannel;
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
                        executor.execute(this::reconnect);
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
        public void send(TransportFrame message) throws IOException {
            Channel channel = channel();
            channel.writeAndFlush(message, channel.voidPromise());
        }

        @Override
        public synchronized void close() {
            closed.set(true);
            for (Channel channel : channels) {
                channel.close();
            }
        }

        private Channel channel() throws IOException {
            int index = (int) channelCounter.get() % channels.length;
            channelCounter.set(index + 1);
            int loopIndex = index;
            do {
                if (channels[index] != null && channels[index].isOpen()) {
                    return channels[index];
                } else if (reconnectScheduled.compareAndSet(false, true)) {
                    executor.execute(this::reconnect);
                }
                index = (index + 1) % channels.length;
            } while (index != loopIndex);
            throw new IOException("error connect to " + node);
        }
    }
}
