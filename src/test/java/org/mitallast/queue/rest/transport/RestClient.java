package org.mitallast.queue.rest.transport;

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.AttributeKey;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.netty.NettyClient;
import org.mitallast.queue.common.settings.Settings;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class RestClient extends NettyClient {

    private final static AttributeKey<ConcurrentLinkedDeque<CompletableFuture<FullHttpResponse>>> attr = AttributeKey.valueOf("queue");
    private final AtomicLong flushCount = new AtomicLong();

    public RestClient(Settings settings) {
        super(settings, RestClient.class, RestClient.class);
    }

    @Override
    protected void init() {
        channel.attr(attr).set(new ConcurrentLinkedDeque<>());
    }

    public CompletableFuture<FullHttpResponse> send(HttpRequest request) {
        final CompletableFuture<FullHttpResponse> future = Futures.future();
        final Channel localChannel = channel;
        localChannel.attr(attr).get().push(future);


        flushCount.incrementAndGet();
        localChannel.write(request, localChannel.voidPromise());
        // automatic flush
        localChannel.pipeline().lastContext().executor().execute(() -> {
            if (flushCount.decrementAndGet() == 0) {
                logger.info("flush");
                localChannel.flush();
            }
        });
        return future;
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("codec", new HttpClientCodec());
                pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
                pipeline.addLast("handler", new SimpleChannelInboundHandler<FullHttpResponse>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) throws Exception {
                        response.content().retain();
                        ctx.channel().attr(attr).get().poll().complete(response);
                    }
                });
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                logger.error("unexpected error {}", ctx, cause);
                ctx.close();
            }
        };
    }
}
