package org.mitallast.queue.common.netty;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.concurrent.TimeUnit;

public abstract class NettyClientBootstrap extends AbstractLifecycleComponent {
    protected final int maxContentLength;
    protected final boolean keepAlive;
    protected final boolean reuseAddress;
    protected final boolean tcpNoDelay;
    protected final int sndBuf;
    protected final int rcvBuf;
    protected final int connectTimeout;

    protected final NettyProvider provider;
    protected volatile Bootstrap bootstrap;

    protected NettyClientBootstrap(Config config, NettyProvider provider) {
        this.provider = provider;
        this.bootstrap = null;

        maxContentLength = config.getInt("netty.max_content_length");
        reuseAddress = config.getBoolean("netty.reuse_address");
        keepAlive = config.getBoolean("netty.keep_alive");
        tcpNoDelay = config.getBoolean("netty.tcp_no_delay");
        sndBuf = config.getInt("netty.snd_buf");
        rcvBuf = config.getInt("netty.rcv_buf");
        connectTimeout = (int) config.getDuration("netty.connect_timeout", TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStart() {
        bootstrap = new Bootstrap()
            .channel(provider.clientChannel())
            .group(provider.child())
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
        Preconditions.checkNotNull(bootstrap);
        return bootstrap.connect(node.getHost(), node.getPort());
    }

    public final ChannelFuture connect(String host, int port) {
        checkIsStarted();
        Preconditions.checkNotNull(bootstrap);
        return bootstrap.connect(host, port);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {

    }
}
