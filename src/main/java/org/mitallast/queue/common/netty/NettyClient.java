package org.mitallast.queue.common.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.concurrent.NamedExecutors;
import org.mitallast.queue.common.settings.Settings;

import java.util.concurrent.ThreadFactory;

public abstract class NettyClient extends AbstractComponent {

    protected final String host;
    protected final int port;
    protected final int threads;
    protected final int maxContentLength;
    protected final boolean keepAlive;
    protected final boolean reuseAddress;
    protected final boolean tcpNoDelay;
    protected final int sndBuf;
    protected final int rcvBuf;
    protected final int wbLow;
    protected final int wbHigh;
    protected volatile Channel channel;
    private volatile Bootstrap bootstrap;

    public NettyClient(Settings settings, Class loggerClass, Class componentClass) {
        super(settings, loggerClass, componentClass);
        host = componentSettings.get("host", "127.0.0.1");
        port = componentSettings.getAsInt("port", defaultPort());
        maxContentLength = componentSettings.getAsInt("max_content_length", 1048576);
        threads = componentSettings.getAsInt("threads", 1);

        reuseAddress = componentSettings.getAsBoolean("reuse_address", false);
        keepAlive = componentSettings.getAsBoolean("keep_alive", true);
        tcpNoDelay = componentSettings.getAsBoolean("tcp_no_delay", true);
        sndBuf = componentSettings.getAsInt("snd_buf", 1048576);
        rcvBuf = componentSettings.getAsInt("rcv_buf", 1048576);
        wbHigh = componentSettings.getAsInt("write_buffer_high_water_mark", 65536);
        wbLow = componentSettings.getAsInt("write_buffer_low_water_mark", 1024);
    }

    protected int defaultPort() {
        return 9080;
    }

    private ThreadFactory threadFactory(String name) {
        return NamedExecutors.newThreadFactory(name);
    }

    public final void start() {
        logger.info("connect to {}:{}", host, port);
        bootstrap = new Bootstrap()
            .channel(NioSocketChannel.class)
            .group(new NioEventLoopGroup(threads, threadFactory("client")))
            .option(ChannelOption.SO_REUSEADDR, reuseAddress)
            .option(ChannelOption.SO_KEEPALIVE, keepAlive)
            .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
            .option(ChannelOption.SO_SNDBUF, sndBuf)
            .option(ChannelOption.SO_RCVBUF, rcvBuf)
            .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, wbHigh)
            .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, wbLow)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(rcvBuf))
            .handler(channelInitializer());

        try {
            channel = bootstrap.connect(host, port).sync().channel();
            init();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void init() {
    }

    public final void stop() {
        if (bootstrap != null) {
            bootstrap.group().shutdownGracefully();
        }
        bootstrap = null;
    }

    public final void flush() {
        channel.flush();
    }

    protected abstract ChannelInitializer channelInitializer();
}
