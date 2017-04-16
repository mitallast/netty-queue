package org.mitallast.queue.common.netty;

import com.typesafe.config.Config;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public abstract class NettyClientBootstrap extends AbstractLifecycleComponent {
    protected final int threads;
    protected final int maxContentLength;
    protected final boolean keepAlive;
    protected final boolean reuseAddress;
    protected final boolean tcpNoDelay;
    protected final int sndBuf;
    protected final int rcvBuf;
    protected final int connectTimeout;

    protected volatile Bootstrap bootstrap;

    public NettyClientBootstrap(Config config) {
        maxContentLength = config.getInt("max_content_length");
        threads = config.getInt("threads");

        reuseAddress = config.getBoolean("reuse_address");
        keepAlive = config.getBoolean("keep_alive");
        tcpNoDelay = config.getBoolean("tcp_no_delay");
        sndBuf = config.getInt("snd_buf");
        rcvBuf = config.getInt("rcv_buf");
        connectTimeout = (int) config.getDuration("connect_timeout", TimeUnit.MILLISECONDS);
    }

    private ThreadFactory threadFactory(String name) {
        return new DefaultThreadFactory(name, true, Thread.NORM_PRIORITY, new ThreadGroup(name));
    }

    @Override
    protected void doStart() {
        final Class<? extends SocketChannel> channelClass;
        final EventLoopGroup group;
        if (Epoll.isAvailable()) {
            logger.info("use epoll");
            channelClass = EpollSocketChannel.class;
            group = new EpollEventLoopGroup(threads, threadFactory("client"));
        } else {
            logger.info("use nio");
            channelClass = NioSocketChannel.class;
            group = new NioEventLoopGroup(threads, threadFactory("client"));
        }
        bootstrap = new Bootstrap()
            .channel(channelClass)
            .group(group)
            .option(ChannelOption.SO_REUSEADDR, reuseAddress)
            .option(ChannelOption.SO_KEEPALIVE, keepAlive)
            .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
            .option(ChannelOption.SO_SNDBUF, sndBuf)
            .option(ChannelOption.SO_RCVBUF, rcvBuf)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
            .option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true))
            .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(65536))
            .handler(channelInitializer());
    }

    protected abstract ChannelInitializer channelInitializer();

    public final ChannelFuture connect(DiscoveryNode node) {
        checkIsStarted();
        return bootstrap.connect(node.host(), node.port());
    }

    public final ChannelFuture connect(String host, int port) {
        checkIsStarted();
        return bootstrap.connect(host, port);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {

    }
}
