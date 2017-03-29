package org.mitallast.queue.common.netty;

import com.typesafe.config.Config;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

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

    public NettyServer(Config config) {
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

    private ThreadFactory threadFactory(String name) {
        return new DefaultThreadFactory(name, true, Thread.NORM_PRIORITY, new ThreadGroup(name));
    }

    @Override
    protected void doStart() throws IOException {
        try {
            final Class<? extends ServerSocketChannel> channelClass;
            final EventLoopGroup boss;
            if(Epoll.isAvailable()){
                logger.info("use epoll");
                channelClass = EpollServerSocketChannel.class;
                boss = new EpollEventLoopGroup(threads, threadFactory("boss"));
            }else {
                logger.info("use nio");
                channelClass = NioServerSocketChannel.class;
                boss = new NioEventLoopGroup(threads, threadFactory("boss"));
            }

            bootstrap = new ServerBootstrap();
            bootstrap.group(boss)
                .channel(channelClass)
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
        bootstrap.config().group().shutdownGracefully();
        channel = null;
        bootstrap = null;
    }

    @Override
    protected void doClose() throws IOException {

    }
}
