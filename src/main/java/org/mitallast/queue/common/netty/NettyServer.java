package org.mitallast.queue.common.netty;

import com.typesafe.config.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;

import java.io.IOException;

public abstract class NettyServer extends AbstractLifecycleComponent {

    protected final String host;
    protected final int port;
    private final int backlog;
    private final boolean keepAlive;
    private final boolean reuseAddress;
    private final boolean tcpNoDelay;
    private final int sndBuf;
    private final int rcvBuf;
    private final int threads;
    protected Channel channel;
    private ServerBootstrap bootstrap;
    private NioEventLoopGroup boss;

    public NettyServer(Config config, Class loggerClass) {
        super(config, loggerClass);
        this.host = config.getString("host");
        this.port = config.getInt("port");
        this.backlog = config.getInt("backlog");
        this.reuseAddress = config.getBoolean("reuse_address");
        this.keepAlive = config.getBoolean("keep_alive");
        this.tcpNoDelay = config.getBoolean("tcp_no_delay");
        this.sndBuf = config.getInt("snd_buf");
        this.rcvBuf = config.getInt("rcv_buf");
        this.threads = config.getInt("threads");
    }

    private NioEventLoopGroup group(String name) {
        return new NioEventLoopGroup(threads, new DefaultThreadFactory(name));
    }

    @Override
    protected void doStart() throws IOException {
        try {
            boss = group("boss");
            bootstrap = new ServerBootstrap();
            bootstrap.group(boss)
                .channel(NioServerSocketChannel.class)
                .childHandler(channelInitializer())
                .option(ChannelOption.SO_BACKLOG, backlog)
                .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .option(ChannelOption.SO_SNDBUF, sndBuf)
                .option(ChannelOption.SO_RCVBUF, rcvBuf)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator());

            channel = bootstrap.bind(host, port)
                .sync()
                .channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    protected abstract ChannelInitializer<SocketChannel> channelInitializer();

    @Override
    protected void doStop() throws IOException {
        try {
            if (channel != null) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        boss.shutdownGracefully();
        channel = null;
        bootstrap = null;
    }

    @Override
    protected void doClose() throws IOException {

    }
}
