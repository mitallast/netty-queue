package org.mitallast.queue.transport.netty;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.Version;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.transport.*;
import org.mitallast.queue.transport.netty.codec.StreamableTransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrameDecoder;
import org.mitallast.queue.transport.netty.codec.TransportFrameEncoder;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class NettyTransportServer extends NettyServer implements TransportServer {

    private final DiscoveryNode discoveryNode;
    private final TransportController transportController;
    private final StreamService streamService;
    private final TransportLocalClient localClient;

    @Inject
    public NettyTransportServer(
            Settings settings,
            TransportController transportController,
            StreamService streamService
    ) {
        super(settings, NettyTransportServer.class, TransportModule.class);
        this.transportController = transportController;
        this.streamService = streamService;
        this.discoveryNode = new DiscoveryNode(
                this.settings.get("node.name"),
                HostAndPort.fromParts(host, port),
                Version.CURRENT
        );
        this.localClient = new TransportLocalClient();
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
    public TransportClient localClient() {
        return localClient;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new TransportServerInitializer(new TransportServerHandler(logger, transportController));
    }

    protected int defaultPort() {
        return 10080;
    }

    private class TransportServerInitializer extends ChannelInitializer<SocketChannel> {

        private final TransportServerHandler transportServerHandler;

        public TransportServerInitializer(TransportServerHandler transportServerHandler) {

            this.transportServerHandler = transportServerHandler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new TransportFrameDecoder(streamService));
            pipeline.addLast(new TransportFrameEncoder(streamService));
            pipeline.addLast(transportServerHandler);
        }
    }

    @ChannelHandler.Sharable
    private class TransportServerHandler extends SimpleChannelInboundHandler<TransportFrame> {

        private final Logger logger;
        private final TransportController transportController;

        public TransportServerHandler(Logger logger, TransportController transportController) {
            super(false);
            this.logger = logger;
            this.transportController = transportController;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, TransportFrame request) {
            if (request.streamable()) {
                TransportChannel channel = new NettyTransportChannel(ctx);
                transportController.dispatchRequest(channel, (StreamableTransportFrame) request);
            } else {
                // ping request
                ctx.writeAndFlush(TransportFrame.of(request.request()), ctx.voidPromise());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("unexpected channel error, close channel", cause);
            ctx.close();
        }
    }

    private class TransportLocalClient implements TransportClient {

        @Override
        public CompletableFuture<TransportFrame> ping() {
            return Futures.complete(TransportFrame.of());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> CompletableFuture<Response> send(Request request) {
            CompletableFuture<Response> future = Futures.future();
            CompletableFuture<Response> actionResponse = transportController.dispatchRequest(request);
            actionResponse.whenComplete((response, error) -> {
                if (error != null) {
                    future.completeExceptionally(error);
                } else if (response.hasError()) {
                    future.completeExceptionally(response.error());
                } else {
                    future.complete(response);
                }
            });
            return future;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> CompletableFuture<Response> forward(Request request) {
            return transportController.dispatchRequest(request);
        }
    }
}
