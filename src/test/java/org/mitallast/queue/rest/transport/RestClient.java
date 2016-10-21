package org.mitallast.queue.rest.transport;

import com.typesafe.config.Config;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.netty.NettyClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

public class RestClient extends NettyClient {

    private final ConcurrentLinkedDeque<CompletableFuture<FullHttpResponse>> queue = new ConcurrentLinkedDeque<>();

    public RestClient(Config config) {
        super(config.getConfig("rest"), RestClient.class);
    }

    public CompletableFuture<FullHttpResponse> send(HttpRequest request) {
        final CompletableFuture<FullHttpResponse> future = Futures.future();
        queue.push(future);
        channel.writeAndFlush(request);
        return future;
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("codec", new HttpClientCodec(4096, 8192, 8192, false, false));
                pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
                pipeline.addLast("handler", new SimpleChannelInboundHandler<FullHttpResponse>(false) {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) throws Exception {
                        queue.poll().complete(response);
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
