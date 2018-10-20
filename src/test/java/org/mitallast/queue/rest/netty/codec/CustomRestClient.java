package org.mitallast.queue.rest.netty.codec;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.internal.PlatformDependent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.netty.NettyClient;
import org.mitallast.queue.common.netty.NettyProvider;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class CustomRestClient extends NettyClient {
    private final Logger logger = LogManager.getLogger();
    private final Queue<CompletableFuture<HttpResponse>> queue = PlatformDependent.newMpscQueue();


    public CustomRestClient(Config config, NettyProvider provider) {
        super(config, provider,
            config.getString("rest.custom.host"),
            config.getInt("rest.custom.port")
        );
    }

    public CompletableFuture<HttpResponse> send(HttpRequest request) {
        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
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
                pipeline.addLast("codec", new HttpRequestEncoder());
                pipeline.addLast("aggregator", new HttpResponseDecoder());
                pipeline.addLast("handler", new SimpleChannelInboundHandler<HttpResponse>(false) {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, HttpResponse response) throws Exception {
//                        logger.info("incoming response");
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
