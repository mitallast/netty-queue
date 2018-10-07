package org.mitallast.queue.rest.transport;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.internal.PlatformDependent;
import org.mitallast.queue.common.netty.NettyClient;
import org.mitallast.queue.common.netty.NettyProvider;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class RestClient extends NettyClient {

    private final Queue<CompletableFuture<FullHttpResponse>> queue = PlatformDependent.newMpscQueue();


    public RestClient(Config config, NettyProvider provider) {
        super(config, provider,
            config.getString("rest.host"),
            config.getInt("rest.port")
        );
    }

    public CompletableFuture<FullHttpResponse> send(HttpRequest request) {
        CompletableFuture<FullHttpResponse> future = new CompletableFuture<>();
        while (!queue.offer(future)) Thread.yield();
        Channel channel = getChannel();
        channel.write(request, channel.voidPromise());
        return future;
    }

    public void flush() {
        getChannel().flush();
    }

    public ByteBufAllocator alloc() {
        return getChannel().alloc();
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("codec", new HttpClientCodec(4096, 8192, 8192, false, false));
                pipeline.addLast("aggregator", new HttpObjectAggregator(getMaxContentLength()));
                pipeline.addLast("handler", new SimpleChannelInboundHandler<FullHttpResponse>(false) {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) throws Exception {
                        queue.poll().complete(response);
                    }
                });
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                getLogger().error("unexpected error {}", ctx, cause);
                ctx.close();
            }
        };
    }
}
