package org.mitallast.queue.common.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.NamedExecutors;
import org.mitallast.queue.common.settings.Settings;

public abstract class NettyServer extends AbstractLifecycleComponent {

    protected final String host;
    protected final int port;
    private final int backlog;
    private final boolean keepAlive;
    private final boolean reuseAddress;
    private final boolean tcpNoDelay;
    private final int sndBuf;
    private final int rcvBuf;
    private final int wbLow;
    private final int wbHigh;
    private final int threads;
    private ServerBootstrap bootstrap;
    private NioEventLoopGroup boss;
    private NioEventLoopGroup worker;
    private Channel channel;

    public NettyServer(Settings settings, Class loggerClass, Class componentClass) {
        super(settings, loggerClass, componentClass);
        this.host = componentSettings.get("host", "127.0.0.1");
        this.port = componentSettings.getAsInt("port", defaultPort());
        this.backlog = componentSettings.getAsInt("backlog", 128);
        this.reuseAddress = componentSettings.getAsBoolean("reuse_address", false);
        this.keepAlive = componentSettings.getAsBoolean("keep_alive", true);
        this.tcpNoDelay = componentSettings.getAsBoolean("tcp_no_delay", true);
        this.sndBuf = componentSettings.getAsInt("snd_buf", 65536);
        this.rcvBuf = componentSettings.getAsInt("rcv_buf", 65536);
        this.wbHigh = componentSettings.getAsInt("write_buffer_high_water_mark", 65536);
        this.wbLow = componentSettings.getAsInt("write_buffer_low_water_mark", 1024);
        this.threads = componentSettings.getAsInt("threads", Runtime.getRuntime().availableProcessors());
    }

    protected int defaultPort() {
        return 8080;
    }

    private NioEventLoopGroup group(String name) {
        return new NioEventLoopGroup(threads, NamedExecutors.newThreadFactory(name));
    }

    @Override
    protected void doStart() throws QueueException {
        try {
            boss = group("boss");
            worker = group("worker");
            bootstrap = new ServerBootstrap();
            bootstrap.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(channelInitializer())
                .option(ChannelOption.SO_BACKLOG, backlog)
                .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .option(ChannelOption.SO_SNDBUF, sndBuf)
                .option(ChannelOption.SO_RCVBUF, rcvBuf)
                .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, wbHigh)
                .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, wbLow)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(rcvBuf))

                .childOption(ChannelOption.SO_REUSEADDR, reuseAddress)
                .childOption(ChannelOption.SO_KEEPALIVE, keepAlive)
                .childOption(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .childOption(ChannelOption.SO_SNDBUF, sndBuf)
                .childOption(ChannelOption.SO_RCVBUF, rcvBuf)
                .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, wbHigh)
                .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, wbLow)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(rcvBuf));
            channel = bootstrap.bind(host, port)
                .sync()
                .channel();
        } catch (InterruptedException e) {
            throw new QueueException(e);
        }
    }

    protected abstract ChannelInitializer<SocketChannel> channelInitializer();

    @Override
    protected void doStop() throws QueueException {
        try {
            if (channel != null) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueueException(e);
        }
        worker.shutdownGracefully();
        boss.shutdownGracefully();
        channel = null;
        bootstrap = null;
    }

    @Override
    protected void doClose() throws QueueException {

    }
}
