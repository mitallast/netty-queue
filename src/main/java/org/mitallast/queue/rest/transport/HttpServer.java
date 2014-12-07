package org.mitallast.queue.rest.transport;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.RestController;

public class HttpServer extends AbstractLifecycleComponent {

    private String host;
    private int port;
    private int backlog;
    private boolean keepAlive;
    private boolean reuseAddress;
    private boolean tcpNoDelay;

    private int sndBuf;
    private int rcvBuf;
    private int wbLow;
    private int wbHigh;

    private RestController restController;

    private ServerBootstrap bootstrap;
    private Channel channel;

    @Inject
    public HttpServer(Settings settings, RestController restController) {
        super(settings);
        this.host = settings.get("host", "127.0.0.1");
        this.port = settings.getAsInt("port", 8080);
        this.backlog = settings.getAsInt("backlog", 1024);
        this.reuseAddress = settings.getAsBoolean("reuse_address", false);
        this.keepAlive = settings.getAsBoolean("keep_alive", true);
        this.tcpNoDelay = settings.getAsBoolean("tcp_no_delay", true);
        this.sndBuf = settings.getAsInt("snd_buf", 4096 * 10);
        this.rcvBuf = settings.getAsInt("rcv_buf", 4096 * 10);
        this.wbHigh = settings.getAsInt("write_buffer_high_water_mark", 64 * 1024);
        this.wbLow = settings.getAsInt("write_buffer_low_water_mark", 2 * 1024);
        this.restController = restController;
    }

    @Override
    protected void doStart() throws QueueException {
        try {
            bootstrap = new ServerBootstrap();
            bootstrap.group(new NioEventLoopGroup())
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new HttpServerInitializer(new HttpServerHandler(restController)))
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
        bootstrap.group().shutdownGracefully();
        channel = null;
        bootstrap = null;
    }

    @Override
    protected void doClose() throws QueueException {

    }
}
