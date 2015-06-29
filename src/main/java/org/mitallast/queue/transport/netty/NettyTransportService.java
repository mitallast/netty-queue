package org.mitallast.queue.transport.netty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.netty.channel.*;
import io.netty.util.AttributeKey;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.client.QueueClient;
import org.mitallast.queue.client.QueuesClient;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.netty.NettyClientBootstrap;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.transport.*;
import org.mitallast.queue.transport.netty.client.TransportQueueClient;
import org.mitallast.queue.transport.netty.client.TransportQueuesClient;
import org.mitallast.queue.transport.netty.codec.StreamableTransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrameDecoder;
import org.mitallast.queue.transport.netty.codec.TransportFrameEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class NettyTransportService extends NettyClientBootstrap implements TransportService {

    private final static AttributeKey<AtomicLong> flushCounterAttr = AttributeKey.valueOf("flushCounter");
    private final static AttributeKey<ConcurrentMap<Long, CompletableFuture>> responseMapAttr = AttributeKey.valueOf("responseMapAttr");
    private final ReentrantLock connectionLock;
    private final int channelCount;
    private final TransportServer transportServer;
    private final TransportController transportController;
    private final StreamService streamService;
    private final List<TransportListener> listeners = new CopyOnWriteArrayList<>();
    private final LocalNodeChannel localNodeChannel;
    private volatile ImmutableMap<DiscoveryNode, NodeChannel> connectedNodes;

    @Inject
    public NettyTransportService(Settings settings, TransportServer transportServer, TransportController transportController, StreamService streamService) {
        super(settings, TransportService.class, TransportModule.class);
        this.transportServer = transportServer;
        this.transportController = transportController;
        this.streamService = streamService;
        channelCount = componentSettings.getAsInt("channel_count", Runtime.getRuntime().availableProcessors());
        connectedNodes = ImmutableMap.of();
        connectionLock = new ReentrantLock();
        localNodeChannel = new LocalNodeChannel();
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new TransportFrameDecoder(streamService));
                pipeline.addLast(new TransportFrameEncoder(streamService));
                pipeline.addLast(new SimpleChannelInboundHandler<TransportFrame>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    protected void channelRead0(ChannelHandlerContext ctx, TransportFrame frame) throws Exception {
                        CompletableFuture future = ctx.attr(responseMapAttr).get().remove(frame.request());
                        if (future == null) {
                            logger.warn("future not found");
                        } else {
                            if (frame instanceof StreamableTransportFrame) {
                                EntryBuilder<? extends EntryBuilder, ActionResponse> builder = ((StreamableTransportFrame) frame).message();
                                future.complete(builder.build());
                            } else {
                                future.complete(frame);
                            }
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
        ImmutableMap<DiscoveryNode, NodeChannel> connectedNodes = this.connectedNodes;
        connectedNodes.keySet().forEach(this::disconnectFromNode);
        super.doStop();
    }

    @Override
    public DiscoveryNode localNode() {
        return transportServer.localNode();
    }

    @Override
    public TransportClient connectToNode(HostAndPort address) {
        if (!lifecycle.started()) {
            throw new IllegalStateException("can't add nodes to stopped transport");
        }
        if (address == null) {
            throw new TransportException("can't connect to null address");
        }
        if (localNode().address().equals(address)) {
            logger.debug("connect to local node");
            return localNodeChannel;
        }

        Channel channel = connect(address).awaitUninterruptibly()
            .channel();
        return new NodeChannel(new Channel[]{channel});
    }

    @Override
    public void connectToNode(DiscoveryNode node) {
        if (!lifecycle.started()) {
            throw new IllegalStateException("can't add nodes to stopped transport");
        }
        if (node == null) {
            throw new TransportException("can't connect to null node");
        }
        if (node.equals(localNode())) {
            logger.debug("connect to local node");
            return;
        }
        boolean connected = false;
        connectionLock.lock();
        try {
            if (connectedNodes.get(node) != null) {
                return;
            }
            logger.debug("connect to node {}", node.name());
            ChannelFuture[] channelFutures = new ChannelFuture[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channelFutures[i] = connect(node.address());
            }

            Channel[] channels = new Channel[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channels[i] = channelFutures[i]
                    .awaitUninterruptibly()
                    .channel();
            }

            NodeChannel nodeChannel = new NodeChannel(channels);
            connectedNodes = ImmutableMap.<DiscoveryNode, NodeChannel>builder()
                .putAll(connectedNodes)
                .put(node, nodeChannel)
                .build();
            connected = true;
            logger.info("connected to node {}", node.name());
        } finally {
            connectionLock.unlock();
            if (connected) {
                listeners.forEach(listener -> listener.connected(node));
            }
        }
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        if (node == null) {
            throw new TransportException("can't disconnect from null node");
        }
        if (node.equals(localNode())) {
            throw new TransportException("can't disconnect from local node");
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
                listeners.forEach(listener -> listener.disconnected(node));
            }
        }
    }

    @Override
    public ImmutableList<DiscoveryNode> connectedNodes() {
        return ImmutableList.copyOf(connectedNodes.keySet());
    }

    @Override
    public TransportClient client(DiscoveryNode node) {
        if (node.equals(localNode())) {
            return localNodeChannel;
        } else {
            NodeChannel nodeChannel = connectedNodes.get(node);
            if (nodeChannel == null) {
                throw new TransportException("Not connected to node: " + node);
            }
            return nodeChannel;
        }
    }

    @Override
    public void addListener(TransportListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(TransportListener listener) {
        listeners.remove(listener);
    }

    private class LocalNodeChannel implements TransportClient {

        private final TransportQueueClient queueClient;
        private final TransportQueuesClient queuesClient;

        private LocalNodeChannel() {
            this.queueClient = new TransportQueueClient(this);
            this.queuesClient = new TransportQueuesClient(this);
        }

        @Override
        public CompletableFuture<TransportFrame> send(TransportFrame frame) {
            CompletableFuture<TransportFrame> future = Futures.future();
            if (frame instanceof StreamableTransportFrame) {
                transportController.dispatchRequest(new TransportChannel() {
                    @Override
                    public void send(TransportFrame response) {
                        future.complete(response);
                    }

                    @Override
                    public void close() {
                        future.completeExceptionally(new IOException("closed"));
                    }
                }, (StreamableTransportFrame) frame);
            } else {
                future.complete(TransportFrame.of(frame.request()));
            }
            return future;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> CompletableFuture<Response> send(Request request) {
            return transportController.dispatchRequest(request);
        }

        @Override
        public QueueClient queue() {
            return queueClient;
        }

        @Override
        public QueuesClient queues() {
            return queuesClient;
        }
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

            for (Channel channel : channels) {
                channel.attr(flushCounterAttr).set(new AtomicLong());
                channel.attr(responseMapAttr).set(new ConcurrentHashMap<>());
            }
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
        public CompletableFuture<TransportFrame> send(TransportFrame frame) {
            Channel channel = channel((int) frame.request());
            CompletableFuture<TransportFrame> future = Futures.future();
            channel.attr(responseMapAttr).get().put(frame.request(), future);
            AtomicLong channelFlushCounter = channel.attr(flushCounterAttr).get();
            channelFlushCounter.incrementAndGet();
            channel.write(frame, channel.voidPromise());
            channel.pipeline().lastContext().executor().execute(() -> {
                if (channelFlushCounter.decrementAndGet() == 0) {
                    if (channel.isOpen()) {
                        channel.flush();
                    }
                }
            });
            return future;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse>
        CompletableFuture<Response> send(Request request) {
            long requestId = channelRequestCounter.incrementAndGet();
            Channel channel = channel((int) requestId);
            CompletableFuture<Response> future = Futures.future();
            StreamableTransportFrame frame = StreamableTransportFrame.of(requestId, request.toBuilder());
            channel.attr(responseMapAttr).get().put(requestId, future);
            AtomicLong channelFlushCounter = channel.attr(flushCounterAttr).get();
            channelFlushCounter.incrementAndGet();
            channel.write(frame, channel.voidPromise());
            channel.pipeline().lastContext().executor().execute(() -> {
                if (channelFlushCounter.decrementAndGet() == 0) {
                    if (channel.isOpen()) {
                        channel.flush();
                    }
                }
            });
            return future;
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
