package org.mitallast.queue.transport.client;

import com.google.inject.Inject;
import io.netty.channel.*;
import io.netty.util.AttributeKey;
import org.mitallast.queue.common.concurrent.futures.Futures;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.netty.NettyClient;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.TransportModule;
import org.mitallast.queue.transport.transport.TransportFrame;
import org.mitallast.queue.transport.transport.TransportFrameDecoder;
import org.mitallast.queue.transport.transport.TransportFrameEncoder;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TransportClient extends NettyClient {

    private final static AttributeKey<ConcurrentMap<Long, SmartFuture<TransportFrame>>> attr = AttributeKey.valueOf("queue");

    @Inject
    public TransportClient(Settings settings) {
        super(settings, TransportClient.class, TransportModule.class);
    }

    @Override
    protected int defaultPort() {
        return 10080;
    }

    @Override
    protected void init() {
        channel.attr(attr).set(new ConcurrentHashMap<>());
    }

    public SmartFuture<TransportFrame> send(TransportFrame frame) throws IOException {
        final SmartFuture<TransportFrame> future = Futures.future();
        final Channel localChannel = channel;
        localChannel.attr(attr).get().put(frame.getRequest(), future);
        localChannel.write(frame, localChannel.voidPromise());
        return future;
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
                        SmartFuture<TransportFrame> future = ctx.attr(attr).get().remove(response.getRequest());
                        if (future == null) {
                            logger.warn("future not found");
                        } else {
                            future.invoke(response);
                        }
                    }
                });
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                cause.printStackTrace();
                ctx.close();
            }
        };
    }
}
