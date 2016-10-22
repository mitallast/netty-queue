package org.mitallast.queue.transport.netty;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.*;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.concurrent.NamedExecutors;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class NettyTransportService extends NettyClientBootstrap implements TransportService {
    private final ReentrantLock connectionLock;
    private final int channelCount;
    private final TransportController transportController;
    private final StreamService streamService;
    private final ExecutorService executorService;
    private volatile ImmutableMap<DiscoveryNode, NodeChannel> connectedNodes;

    @Inject
    public NettyTransportService(Config config, TransportController transportController, StreamService streamService) {
        super(config.getConfig("transport"), TransportService.class);
        this.transportController = transportController;
        this.streamService = streamService;
        channelCount = this.config.getInt("channel_count");
        connectedNodes = ImmutableMap.of();
        connectionLock = new ReentrantLock();
        executorService = NamedExecutors.newSingleThreadPool("reconnect");
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
                    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
                        ctx.channel().attr(NettyTransportChannel.channelAttr).set(new NettyTransportChannel(ctx));
                    }

                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, TransportFrame frame) throws Exception {
                        NettyTransportChannel transportChannel = ctx.channel().attr(NettyTransportChannel.channelAttr).get();
                        transportController.dispatch(transportChannel, frame);
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
        List<Runnable> tasks = executorService.shutdownNow();
        logger.warn("not executed tasks {}", tasks);
        ImmutableMap<DiscoveryNode, NodeChannel> connectedNodes = this.connectedNodes;
        connectedNodes.keySet().forEach(this::disconnectFromNode);
        super.doStop();
    }

    @Override
    protected void doClose() throws IOException {
        super.doClose();
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void connectToNode(DiscoveryNode node) {
        checkIsStarted();
        if (node == null) {
            throw new IllegalArgumentException("can't connect to null node");
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
        if (node == null) {
            throw new IllegalArgumentException("can't disconnect from null node");
        }
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
        NodeChannel nodeChannel = connectedNodes.get(node);
        if (nodeChannel == null) {
            throw new IllegalArgumentException("Not connected to node: " + node);
        }
        return nodeChannel;
    }

    private class NodeChannel implements TransportChannel, Closeable {
        private final DiscoveryNode node;
        private final AtomicLong channelRequestCounter = new AtomicLong();
        private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Channel[] channels;

        private NodeChannel(DiscoveryNode node) {
            this.node = node;
            this.channels = new Channel[channelCount];
        }

        private synchronized void open() {
            logger.debug("connect to {}", node);
            ChannelFuture[] channelFutures = new ChannelFuture[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channelFutures[i] = connect(node);
            }
            logger.debug("await channel open {}", node);
            for (int i = 0; i < channelCount; i++) {
                try {
                    Channel channel = channelFutures[i]
                            .awaitUninterruptibly()
                            .channel();
                    channels[i] = channel;
                } catch (Throwable e) {
                    logger.error("error connect to {}", node, e);
                    if (reconnectScheduled.compareAndSet(false, true)) {
                        executorService.execute(this::reconnect);
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
        public void send(TransportFrame message) {
            long requestId = channelRequestCounter.incrementAndGet();
            Channel channel = channel((int) requestId);
            if (channel == null) {
                return;
            }
            channel.writeAndFlush(message, channel.voidPromise());
        }

        @Override
        public synchronized void close() {
            closed.set(true);
            List<ChannelFuture> closeFutures = new ArrayList<>(channels.length);
            for (Channel channel : channels) {
                if (channel != null && channel.isOpen()) {
                    closeFutures.add(channel.close());
                }
            }
            for (ChannelFuture closeFuture : closeFutures) {
                try {
                    closeFuture.awaitUninterruptibly();
                } catch (Exception e) {
                    //ignore
                }
            }
        }

        private Channel channel(int request) {
            int index = Math.abs(request % channels.length);
            int loopIndex = index;
            do {
                index--;
                if (index < 0) {
                    index += channels.length;
                }
                if (channels[index] != null && channels[index].isOpen()) {
                    return channels[index];
                } else if (reconnectScheduled.compareAndSet(false, true)) {
                    executorService.execute(this::reconnect);
                }
            } while (index != loopIndex);
            return null;
        }
    }
}
