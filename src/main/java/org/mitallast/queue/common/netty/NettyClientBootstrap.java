package org.mitallast.queue.common.netty;

import com.google.common.net.HostAndPort;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.NamedExecutors;
import org.mitallast.queue.common.settings.Settings;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

public abstract class NettyClientBootstrap extends AbstractLifecycleComponent {
    protected final int threads;
    protected final int maxContentLength;
    protected final boolean keepAlive;
    protected final boolean reuseAddress;
    protected final boolean tcpNoDelay;
    protected final int sndBuf;
    protected final int rcvBuf;
    protected final int wbLow;
    protected final int wbHigh;

    private volatile Bootstrap bootstrap;

    public NettyClientBootstrap(Settings settings, Class loggerClass, Class componentClass) {
        super(settings, loggerClass, componentClass);
        maxContentLength = componentSettings.getAsInt("max_content_length", 1048576);
        threads = componentSettings.getAsInt("threads", Runtime.getRuntime().availableProcessors());

        reuseAddress = componentSettings.getAsBoolean("reuse_address", false);
        keepAlive = componentSettings.getAsBoolean("keep_alive", true);
        tcpNoDelay = componentSettings.getAsBoolean("tcp_no_delay", true);
        sndBuf = componentSettings.getAsInt("snd_buf", 1048576);
        rcvBuf = componentSettings.getAsInt("rcv_buf", 1048576);
        wbHigh = componentSettings.getAsInt("write_buffer_high_water_mark", 65536);
        wbLow = componentSettings.getAsInt("write_buffer_low_water_mark", 1024);
    }

    private ThreadFactory threadFactory(String name) {
        return NamedExecutors.newThreadFactory(name);
    }

    @Override
    protected void doStart() throws IOException {
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
    }

    protected abstract ChannelInitializer channelInitializer();

    public final ChannelFuture connect(HostAndPort address) {
        return bootstrap.connect(address.getHostText(), address.getPort());
    }

    public final ChannelFuture connect(String host, int port) {
        return bootstrap.connect(host, port);
    }

    @Override
    protected void doStop() throws IOException {
        if (bootstrap != null) {
            bootstrap.group().shutdownGracefully();
        }
        bootstrap = null;
    }

    @Override
    protected void doClose() throws IOException {

    }
}
