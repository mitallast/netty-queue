package org.mitallast.queue.rest;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.AttributeKey;
import org.mitallast.queue.common.concurrent.BasicFuture;
import org.mitallast.queue.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;

public class RestClient {
    private final static Logger logger = LoggerFactory.getLogger(RestClient.class);
    private final static AttributeKey<ConcurrentLinkedDeque<BasicFuture<FullHttpResponse>>> attr = AttributeKey.valueOf("queue");
    private final String host;
    private final int port;
    private final int threads;
    private final int maxContentLength;
    private final boolean useOio;
    private Bootstrap bootstrap;
    private Channel httpChannel;
    private boolean keepAlive;
    private boolean reuseAddress;
    private boolean tcpNoDelay;
    private int sndBuf;
    private int rcvBuf;
    private int wbLow;
    private int wbHigh;

    public RestClient(Settings settings) {

        host = settings.get("host", "127.0.0.1");
        port = settings.getAsInt("port", 8080);
        maxContentLength = settings.getAsInt("max_content_length", 1048576);
        threads = settings.getAsInt("threads", 24);
        useOio = settings.getAsBoolean("use_oio", false);

        reuseAddress = settings.getAsBoolean("reuse_address", true);
        keepAlive = settings.getAsBoolean("keep_alive", true);
        tcpNoDelay = settings.getAsBoolean("tcp_no_delay", false);
        sndBuf = settings.getAsInt("snd_buf", 65536);
        rcvBuf = settings.getAsInt("rcv_buf", 65536);
        wbHigh = settings.getAsInt("write_buffer_high_water_mark", 65536);
        wbLow = settings.getAsInt("write_buffer_low_water_mark", 1024);
    }

    public void start() {
        bootstrap = new Bootstrap()
                .channel(useOio ? OioSocketChannel.class : NioSocketChannel.class)
                .group(useOio ? new OioEventLoopGroup(threads) : new NioEventLoopGroup(threads))
                .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .option(ChannelOption.SO_SNDBUF, sndBuf)
                .option(ChannelOption.SO_RCVBUF, rcvBuf)
                .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, wbHigh)
                .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, wbLow)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(rcvBuf))
                .handler(new ChannelInitializer<SocketChannel>() {

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
                });

        try {
            httpChannel = bootstrap.connect(host, port).sync().channel();
            httpChannel.attr(attr).set(new ConcurrentLinkedDeque<BasicFuture<FullHttpResponse>>());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        if (bootstrap != null) {
            bootstrap.group().shutdownGracefully();
        }
    }

    public Future<FullHttpResponse> send(HttpRequest request) {
        final BasicFuture<FullHttpResponse> future = new BasicFuture<>();
        httpChannel.attr(attr).get().push(future);
        httpChannel.writeAndFlush(request);
        return future;
    }
}
