package org.mitallast.queue.transport.http;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.RestController;

public class HttpServer extends AbstractLifecycleComponent {

    private int port;
    private int backlog;
    private boolean keepAlive;
    private boolean reuseAddress;
    private boolean tcpNoDelay;

    private RestController restController;


    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap bootstrap;
    private Channel channel;

    @Inject
    public HttpServer(Settings settings, RestController restController) {
        super(settings);
        this.port = settings.getAsInt("port", 8080);
        this.backlog = settings.getAsInt("backlog", 65536);
        this.reuseAddress = settings.getAsBoolean("reuse_address", true);
        this.keepAlive = settings.getAsBoolean("keep_alive", true);
        this.tcpNoDelay = settings.getAsBoolean("tcp_no_delay", true);
        this.restController = restController;
    }

    @Override
    protected void doStart() throws QueueException {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try {
            bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new HttpServerInitializer(new HttpServerHandler(restController)))
                    .option(ChannelOption.SO_BACKLOG, backlog)
                    .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                    .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                    .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT);
            channel = bootstrap.bind(port).sync().channel();
        } catch (InterruptedException e) {
            throw new QueueException(e);
        }
    }

    @Override
    protected void doStop() throws QueueException {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        try {
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            throw new QueueException(e);
        }
        channel = null;
        bootstrap = null;
        workerGroup = null;
        bossGroup = null;
    }

    @Override
    protected void doClose() throws QueueException {

    }
}
