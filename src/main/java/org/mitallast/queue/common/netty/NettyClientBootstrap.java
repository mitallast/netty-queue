package org.mitallast.queue.common.netty;

import com.typesafe.config.Config;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.transport.DiscoveryNode;

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

    protected volatile Bootstrap bootstrap;

    public NettyClientBootstrap(Config config, Class loggerClass) {
        super(config, loggerClass);
        maxContentLength = config.getInt("max_content_length");
        threads = config.getInt("threads");

        reuseAddress = config.getBoolean("reuse_address");
        keepAlive = config.getBoolean("keep_alive");
        tcpNoDelay = config.getBoolean("tcp_no_delay");
        sndBuf = config.getInt("snd_buf");
        rcvBuf = config.getInt("rcv_buf");
    }

    private ThreadFactory threadFactory(String name) {
        return new DefaultThreadFactory(name);
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
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator())
            .handler(channelInitializer());
    }

    protected abstract ChannelInitializer channelInitializer();

    public final ChannelFuture connect(DiscoveryNode node) {
        return bootstrap.connect(node.host(), node.port());
    }

    public final ChannelFuture connect(String host, int port) {
        return bootstrap.connect(host, port);
    }

    @Override
    protected void doStop() throws IOException {
    }

    @Override
    protected void doClose() throws IOException {

    }
}
