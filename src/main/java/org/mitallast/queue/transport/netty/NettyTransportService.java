package org.mitallast.queue.transport.netty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.AttributeKey;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.client.QueueClient;
import org.mitallast.queue.client.QueuesClient;
import org.mitallast.queue.common.concurrent.futures.Futures;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.event.EventListener;
import org.mitallast.queue.common.event.EventObserver;
import org.mitallast.queue.common.netty.NettyClientBootstrap;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.ByteBufStreamOutput;
import org.mitallast.queue.transport.*;
import org.mitallast.queue.transport.netty.client.TransportQueueClient;
import org.mitallast.queue.transport.netty.client.TransportQueuesClient;
import org.mitallast.queue.transport.netty.codec.TransportFrameDecoder;
import org.mitallast.queue.transport.netty.codec.TransportFrameEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class NettyTransportService extends NettyClientBootstrap implements TransportService {

    private final static AttributeKey<AtomicLong> flushCounterAttr = AttributeKey.valueOf("flushCounter");
    private final static AttributeKey<ConcurrentMap<Long, SmartFuture<TransportFrame>>> responseMapAttr = AttributeKey.valueOf("responseMapAttr");
    private final ReentrantLock connectionLock;
    private final int channelCount;
    private final TransportServer transportServer;
    private final EventObserver<NodeConnectedEvent> nodeConnectedObserver = EventObserver.create();
    private final EventObserver<NodeDisconnectedEvent> nodeDisconnectedObserver = EventObserver.create();
    private volatile ImmutableMap<DiscoveryNode, NodeChannel> connectedNodes;

    @Inject
    public NettyTransportService(Settings settings, TransportServer transportServer) {
        super(settings, TransportService.class, TransportModule.class);
        this.transportServer = transportServer;
        channelCount = componentSettings.getAsInt("channel_count", Runtime.getRuntime().availableProcessors());
        connectedNodes = ImmutableMap.of();
        connectionLock = new ReentrantLock();
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new TransportFrameDecoder());
                pipeline.addLast(new TransportFrameEncoder());
                pipeline.addLast(new SimpleChannelInboundHandler<TransportFrame>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, TransportFrame response) throws Exception {
                        SmartFuture<TransportFrame> future = ctx.attr(responseMapAttr).get().remove(response.getRequest());
                        if (future == null) {
                            logger.warn("future not found");
                        } else {
                            future.invoke(response);
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
    protected void doStop() throws IOException {
        final ImmutableMap<DiscoveryNode, NodeChannel> connectedNodes = this.connectedNodes;
        connectedNodes.keySet().forEach(this::disconnectFromNode);
        super.doStop();
    }

    @Override
    public DiscoveryNode localNode() {
        return transportServer.localNode();
    }

    @Override
    public void connectToNode(DiscoveryNode node) {
        if (!lifecycle.started()) {
            throw new IllegalStateException("can't add nodes to a stopped transport");
        }
        if (node == null) {
            throw new TransportException("can't connect to a null node");
        }
        boolean connected = false;
        connectionLock.lock();
        try {
            if (connectedNodes.get(node) != null) {
                return;
            }
            logger.debug("connect to node {}", node.nodeName());
            ChannelFuture[] channelFutures = new ChannelFuture[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channelFutures[i] = connect(node.getHost(), node.getPort());
            }

            Channel[] channels = new Channel[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channels[i] = channelFutures[i]
                    .awaitUninterruptibly()
                    .channel();

                channels[i].attr(flushCounterAttr).set(new AtomicLong());
                channels[i].attr(responseMapAttr).set(new ConcurrentHashMap<>());
            }

            NodeChannel nodeChannel = new NodeChannel(channels);
            connectedNodes = ImmutableMap.<DiscoveryNode, NodeChannel>builder()
                .putAll(connectedNodes)
                .put(node, nodeChannel)
                .build();
            connected = true;
            logger.info("connected to node {}", node.nodeName());
        } finally {
            connectionLock.unlock();
            if (connected) {
                nodeConnectedObserver.triggerEvent(new NodeConnectedEvent(node));
            }
        }
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        if (node == null) {
            throw new TransportException("can't disconnect to a null node");
        }
        boolean disconnected = false;
        connectionLock.lock();
        try {
            NodeChannel nodeChannel = connectedNodes.get(node);
            if (nodeChannel == null) {
                return;
            }
            logger.info("disconnect from node {}", node);
            nodeChannel.close();
            ImmutableMap.Builder<DiscoveryNode, NodeChannel> builder = ImmutableMap.builder();
            connectedNodes.forEach((nodeItem, nodeChannelItem) -> {
                if (!nodeItem.equals(node)) {
                    builder.put(nodeItem, nodeChannelItem);
                }
            });
            connectedNodes = builder.build();
            disconnected = true;
        } finally {
            connectionLock.unlock();
            if (disconnected) {
                nodeDisconnectedObserver.triggerEvent(new NodeDisconnectedEvent(node));
            }
        }
    }

    @Override
    public ImmutableList<DiscoveryNode> connectedNodes() {
        return ImmutableList.copyOf(connectedNodes.keySet());
    }

    @Override
    public SmartFuture<TransportFrame> sendRequest(DiscoveryNode node, TransportFrame frame) {
        NodeChannel nodeChannel = connectedNodes.get(node);
        if (nodeChannel == null) {
            throw new TransportException("Not connected to node: " + node);
        }
        Channel channel = nodeChannel.channel((int) frame.getRequest());
        final SmartFuture<TransportFrame> future = Futures.future();
        channel.attr(responseMapAttr).get().put(frame.getRequest(), future);
        AtomicLong channelFlushCounter = channel.attr(flushCounterAttr).get();
        channelFlushCounter.incrementAndGet();
        channel.write(frame, channel.voidPromise());
        channel.pipeline().lastContext().executor().execute(() -> {
            if (channelFlushCounter.decrementAndGet() == 0) {
                channel.flush();
            }
        });
        return future;
    }

    @Override
    public TransportClient client(DiscoveryNode node) {
        NodeChannel nodeChannel = connectedNodes.get(node);
        if (nodeChannel == null) {
            throw new TransportException("Not connected to node: " + node);
        }
        return nodeChannel;
    }

    @Override
    public void addNodeConnectedListener(EventListener<NodeConnectedEvent> listener) {
        nodeConnectedObserver.addListener(listener);
    }

    @Override
    public void removeNodeConnectedListener(EventListener<NodeConnectedEvent> listener) {
        nodeConnectedObserver.removeListener(listener);
    }

    @Override
    public void addNodeDisconnectedListener(EventListener<NodeDisconnectedEvent> listener) {
        nodeDisconnectedObserver.addListener(listener);
    }

    @Override
    public void removeNodeDisconnectedListener(EventListener<NodeDisconnectedEvent> listener) {
        nodeDisconnectedObserver.removeListener(listener);
    }

    private class NodeChannel implements TransportClient, Closeable {
        private final AtomicLong channelRequestCounter = new AtomicLong();

        private final Channel[] channels;
        private final TransportQueuesClient queuesClient;

        private final TransportQueueClient queueClient;

        private NodeChannel(Channel[] channels) {
            this.channels = channels;
            this.queueClient = new TransportQueueClient(this);
            this.queuesClient = new TransportQueuesClient(this);
        }

        @Override
        public synchronized void close() {
            List<ChannelFuture> closeFutures = new ArrayList<>(channels.length);
            for (Channel channel : channels) {
                if (channel.isOpen()) {
                    channel.flush();
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

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse>
        SmartFuture<Response> send(String actionName, Request request, ResponseMapper<Response> mapper) {
            long requestId = channelRequestCounter.incrementAndGet();
            Channel channel = channel((int) requestId);
            ByteBuf buffer = channel.alloc().ioBuffer();
            try (ByteBufStreamOutput streamOutput = new ByteBufStreamOutput(buffer)) {
                streamOutput.writeText(actionName);
                request.writeTo(streamOutput);
            } catch (IOException e) {
                logger.error("error write", e);
                return Futures.future(e);
            }

            TransportFrame frame = TransportFrame.of(requestId, buffer);
            final SmartFuture<TransportFrame> future = Futures.future();
            channel.attr(responseMapAttr).get().put(frame.getRequest(), future);
            AtomicLong channelFlushCounter = channel.attr(flushCounterAttr).get();
            channelFlushCounter.incrementAndGet();
            channel.write(frame, channel.voidPromise());
            channel.pipeline().lastContext().executor().execute(() -> {
                if (channelFlushCounter.decrementAndGet() == 0) {
                    channel.flush();
                }
            });
            return future.map(mapper);
        }

        private Channel channel(int request) {
            return channels[Math.abs(request % channels.length)];
        }

        @Override
        public QueuesClient queues() {
            return queuesClient;
        }

        @Override
        public QueueClient queue() {
            return queueClient;
        }

    }
}
