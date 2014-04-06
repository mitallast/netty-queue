package org.mitallast.queue.transport.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.RestController;

import java.io.IOException;

public class HttpServer {
    private int port;
    private int backlog;
    private boolean keepAlive;
    private boolean reuseAddress;
    private boolean tcpNoDelay;

    private RestController restController;

    public HttpServer(Settings componentSettings, RestController restController) {
        this.port = componentSettings.getAsInt("port", 8080);
        this.backlog = componentSettings.getAsInt("backlog", 65536);
        this.reuseAddress = componentSettings.getAsBoolean("reuse_address", true);
        this.keepAlive = componentSettings.getAsBoolean("keep_alive", true);
        this.tcpNoDelay = componentSettings.getAsBoolean("tcp_no_delay", true);
        this.restController = restController;
    }

    public void run() throws IOException, InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new HttpServerInitializer(new HttpServerHandler(restController)))
                    .option(ChannelOption.SO_BACKLOG, backlog)
                    .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                    .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                    .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT);

            Channel ch = b.bind(port).sync().channel();
            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
