package org.mitallast.queue.common.netty;

import com.typesafe.config.Config;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;

import javax.inject.Inject;
import java.util.concurrent.ThreadFactory;

public class NettyProvider extends AbstractLifecycleComponent {
    private final Class<? extends ServerSocketChannel> serverChannel;
    private final Class<? extends SocketChannel> clientChannel;
    private final EventLoopGroup parent;
    private final EventLoopGroup child;

    @Inject
    public NettyProvider(Config config) {
        int parentThreads = config.getInt("netty.threads.parent");
        int childThreads = config.getInt("netty.threads.child");

        ThreadFactory parentTF = threadFactory("parent");
        ThreadFactory childTF = threadFactory("child");

        if (Epoll.isAvailable()) {
            logger.info("use epoll");
            serverChannel = EpollServerSocketChannel.class;
            clientChannel = EpollSocketChannel.class;
            parent = new EpollEventLoopGroup(parentThreads, parentTF);
            child = new EpollEventLoopGroup(childThreads, childTF);
        } else {
            logger.info("use nio");
            serverChannel = NioServerSocketChannel.class;
            clientChannel = NioSocketChannel.class;
            parent = new NioEventLoopGroup(parentThreads, parentTF);
            child = new NioEventLoopGroup(childThreads, childTF);
        }
    }

    private ThreadFactory threadFactory(String name) {
        return new DefaultThreadFactory(name, true, Thread.NORM_PRIORITY, new ThreadGroup(name));
    }

    public Class<? extends ServerSocketChannel> serverChannel() {
        return serverChannel;
    }

    public Class<? extends SocketChannel> clientChannel() {
        return clientChannel;
    }

    public EventLoopGroup parent() {
        return parent;
    }

    public EventLoopGroup child() {
        return child;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        child.shutdownGracefully();
        parent.shutdownGracefully();
    }
}
