package org.mitallast.queue.rest.transport;

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.AttributeKey;
import org.mitallast.queue.common.concurrent.BasicFuture;
import org.mitallast.queue.common.netty.NettyClient;
import org.mitallast.queue.common.settings.Settings;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;

public class RestClient extends NettyClient {

    private final static AttributeKey<ConcurrentLinkedDeque<BasicFuture<FullHttpResponse>>> attr = AttributeKey.valueOf("queue");

    public RestClient(Settings settings) {
        super(settings);
    }

    @Override
    protected void init() {
        channel.attr(attr).set(new ConcurrentLinkedDeque<BasicFuture<FullHttpResponse>>());
    }

    public Future<FullHttpResponse> send(HttpRequest request) {
        return send(request, false);
    }

    public Future<FullHttpResponse> send(HttpRequest request, boolean flush) {
        final BasicFuture<FullHttpResponse> future = new BasicFuture<>();
        final Channel localChannel = channel;
        localChannel.attr(attr).get().push(future);
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
                pipeline.addLast("codec", new HttpClientCodec());
                pipeline.addLast("aggregator", new HttpObjectAggregator(maxContentLength));
                pipeline.addLast("handler", new SimpleChannelInboundHandler<FullHttpResponse>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) throws Exception {
                        response.content().retain();
                        ctx.channel().attr(attr).get().poll().set(response);
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
