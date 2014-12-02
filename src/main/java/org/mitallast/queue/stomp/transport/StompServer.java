package org.mitallast.queue.stomp.transport;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.stomp.StompController;

public class StompServer extends AbstractLifecycleComponent {

    private final StompController stompController;
    private String host;
    private int port;
    private int backlog;
    private boolean keepAlive;
    private boolean reuseAddress;
    private boolean tcpNoDelay;
    private EventLoopGroup bossGroup;
    private Class<? extends ServerChannel> channelClass;
    private ServerBootstrap bootstrap;
    private Channel channel;

    @Inject
    public StompServer(Settings settings, StompController stompController) {
        super(settings);
        this.host = settings.get("host", "127.0.0.1");
        this.port = settings.getAsInt("port", 9080);
        this.backlog = settings.getAsInt("backlog", 65536);
        this.reuseAddress = settings.getAsBoolean("reuse_address", true);
        this.keepAlive = settings.getAsBoolean("keep_alive", true);
        this.tcpNoDelay = settings.getAsBoolean("tcp_no_delay", false);
        this.stompController = stompController;
    }

    @Override
    protected void doStart() throws QueueException {
        bossGroup = new NioEventLoopGroup();
        channelClass = NioServerSocketChannel.class;
        try {
            bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup)
                .channel(channelClass)
                .childHandler(new StompServerInitializer(new StompServerHandler(stompController)))
                .option(ChannelOption.SO_BACKLOG, backlog)
                .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT);
            channel = bootstrap.bind(host, port)
                .sync()
                .channel();
        } catch (InterruptedException e) {
            throw new QueueException(e);
        }
    }

    @Override
    protected void doStop() throws QueueException {
        bossGroup.shutdownGracefully();
        try {
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            throw new QueueException(e);
        }
        channel = null;
        bootstrap = null;
        bossGroup = null;
    }

    @Override
    protected void doClose() throws QueueException {

    }
}
