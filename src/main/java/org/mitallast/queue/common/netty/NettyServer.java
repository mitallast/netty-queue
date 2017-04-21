package org.mitallast.queue.common.netty;

import com.typesafe.config.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;

public abstract class NettyServer extends AbstractLifecycleComponent {
    protected final String host;
    protected final int port;
    private final int backlog;
    private final boolean keepAlive;
    private final boolean reuseAddress;
    private final boolean tcpNoDelay;
    private final int sndBuf;
    private final int rcvBuf;
    protected final NettyProvider provider;
    protected Channel channel;
    protected ServerBootstrap bootstrap;

    protected NettyServer(Config config, NettyProvider provider, String host, int port) {
        this.host = host;
        this.port = port;
        this.provider = provider;
        this.backlog = config.getInt("netty.backlog");
        this.reuseAddress = config.getBoolean("netty.reuse_address");
        this.keepAlive = config.getBoolean("netty.keep_alive");
        this.tcpNoDelay = config.getBoolean("netty.tcp_no_delay");
        this.sndBuf = config.getInt("netty.snd_buf");
        this.rcvBuf = config.getInt("netty.rcv_buf");
    }

    @Override
    protected void doStart() {
        try {
            bootstrap = new ServerBootstrap();
            bootstrap.group(provider.parent(), provider.child())
                .channel(provider.serverChannel())
                .handler(channelInitializer())
                .childHandler(channelInitializer())
                .option(ChannelOption.SO_BACKLOG, backlog)
                .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .option(ChannelOption.SO_SNDBUF, sndBuf)
                .option(ChannelOption.SO_RCVBUF, rcvBuf)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator());

            logger.info("listen {}:{}", host, port);
            channel = bootstrap.bind(host, port)
                .sync()
                .channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    protected abstract ChannelInitializer channelInitializer();

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        try {
            if (channel != null) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        channel = null;
        bootstrap = null;
    }
}
