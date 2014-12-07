package org.mitallast.queue.stomp;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.codec.stomp.*;
import org.mitallast.queue.common.concurrent.BasicFuture;
import org.mitallast.queue.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class StompClient {
    private final static Logger logger = LoggerFactory.getLogger(StompClient.class);
    private final String host;
    private final int port;
    private final int threads;
    private final int maxContentLength;
    private final boolean useOio;
    private final ConcurrentHashMap<String, BasicFuture<StompFrame>> frameFutureMap = new ConcurrentHashMap<>();
    private Bootstrap stompBootstrap;
    private Channel stompChannel;
    private boolean keepAlive;
    private boolean reuseAddress;
    private boolean tcpNoDelay;
    private int sndBuf;
    private int rcvBuf;
    private int wbLow;
    private int wbHigh;

    public StompClient(Settings settings) {
        host = settings.get("host", "127.0.0.1");
        port = settings.getAsInt("port", 9080);
        maxContentLength = settings.getAsInt("max_content_length", 1048576);
        threads = settings.getAsInt("threads", 24);
        useOio = settings.getAsBoolean("use_oio", false);

        reuseAddress = settings.getAsBoolean("reuse_address", false);
        keepAlive = settings.getAsBoolean("keep_alive", true);
        tcpNoDelay = settings.getAsBoolean("tcp_no_delay", true);
        sndBuf = settings.getAsInt("snd_buf", 4096 * 10);
        rcvBuf = settings.getAsInt("rcv_buf", 4096 * 10);
        wbHigh = settings.getAsInt("write_buffer_high_water_mark", 64 * 1024);
        wbLow = settings.getAsInt("write_buffer_low_water_mark", 2 * 1024);
    }

    public void start() {
        stompBootstrap = new Bootstrap()
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
            });

        try {
            stompChannel = stompBootstrap.connect(host, port).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        if (stompBootstrap != null) {
            stompBootstrap.group().shutdownGracefully();
        }
    }

    public Future<StompFrame> send(StompFrame request) {
        String receipt = request.headers().get(StompHeaders.RECEIPT);
        BasicFuture<StompFrame> future = new BasicFuture<>();
        frameFutureMap.put(receipt, future);
        stompChannel.writeAndFlush(request);
        return future;
    }

    private class StompHandler extends SimpleChannelInboundHandler<StompFrame> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, StompFrame frame) throws Exception {
            String receipt = frame.headers().get(StompHeaders.RECEIPT_ID);
            BasicFuture<StompFrame> frameFuture = frameFutureMap.remove(receipt);
            if (frameFuture != null) {
                frameFuture.set(frame);
            } else {
                logger.warn("handler not found: " + receipt);
            }
        }
    }
}
