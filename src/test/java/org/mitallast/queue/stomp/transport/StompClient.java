package org.mitallast.queue.stomp.transport;

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.stomp.*;
import io.netty.util.AttributeKey;
import org.mitallast.queue.common.Strings;
import org.mitallast.queue.common.concurrent.BasicFuture;
import org.mitallast.queue.common.netty.NettyClient;
import org.mitallast.queue.common.settings.Settings;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class StompClient extends NettyClient {

    protected final static AttributeKey<ConcurrentHashMap<String, BasicFuture<StompFrame>>> attr = AttributeKey.valueOf("map");

    public StompClient(Settings settings) {
        super(settings);
    }

    @Override
    protected void init() {
        channel.attr(attr).set(new ConcurrentHashMap<>());
    }

    public Future<StompFrame> send(StompFrame request) {
        return send(request, true);
    }

    public Future<StompFrame> send(StompFrame request, boolean flush) {
        String receipt = Strings.toString(request.headers().get(StompHeaders.RECEIPT));
        assert !Strings.isEmpty(receipt);
        BasicFuture<StompFrame> future = new BasicFuture<>();
        Channel localChannel = channel;
        localChannel.attr(attr).get().put(receipt, future);
        if (flush) {
            localChannel.writeAndFlush(request, localChannel.voidPromise());
        } else {
            localChannel.write(request, localChannel.voidPromise());
        }
        return future;
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("decoder", new StompSubframeDecoder());
                pipeline.addLast("encoder", new StompSubframeEncoder());
                pipeline.addLast("aggregator", new StompSubframeAggregator(maxContentLength));
                pipeline.addLast("handler", new StompHandler());
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                cause.printStackTrace();
                ctx.close();
            }
        };
    }

    private class StompHandler extends SimpleChannelInboundHandler<StompFrame> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, StompFrame frame) throws Exception {
            String receipt = Strings.toString(frame.headers().get(StompHeaders.RECEIPT_ID));
            assert !Strings.isEmpty(receipt);
            BasicFuture<StompFrame> frameFuture = ctx.channel().attr(attr).get().remove(receipt);
            if (frameFuture != null) {
                frameFuture.set(frame);
            } else {
                logger.warn("handler not found: " + receipt);
            }
        }
    }
}
