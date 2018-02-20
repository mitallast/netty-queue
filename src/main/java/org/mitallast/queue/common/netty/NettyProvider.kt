package org.mitallast.queue.common.netty

import com.typesafe.config.Config
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.epoll.EpollSocketChannel
import io.netty.channel.kqueue.KQueue
import io.netty.channel.kqueue.KQueueEventLoopGroup
import io.netty.channel.kqueue.KQueueServerSocketChannel
import io.netty.channel.kqueue.KQueueSocketChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import org.mitallast.queue.common.component.AbstractLifecycleComponent
import java.util.concurrent.ThreadFactory
import javax.inject.Inject

class NettyProvider @Inject
constructor(config: Config) : AbstractLifecycleComponent() {
    private val serverChannel: Class<out ServerSocketChannel>
    private val clientChannel: Class<out SocketChannel>
    private val parent: EventLoopGroup
    private val child: EventLoopGroup

    init {
        val parentThreads = config.getInt("netty.threads.parent")
        val childThreads = config.getInt("netty.threads.child")

        val parentTF = threadFactory("parent")
        val childTF = threadFactory("child")

        if (Epoll.isAvailable()) {
            logger.info("use epoll")
            serverChannel = EpollServerSocketChannel::class.java
            clientChannel = EpollSocketChannel::class.java
            parent = EpollEventLoopGroup(parentThreads, parentTF)
            child = EpollEventLoopGroup(childThreads, childTF)
        } else if (KQueue.isAvailable()) {
            logger.info("use kqueue")
            serverChannel = KQueueServerSocketChannel::class.java
            clientChannel = KQueueSocketChannel::class.java
            parent = KQueueEventLoopGroup(parentThreads, parentTF)
            child = KQueueEventLoopGroup(childThreads, childTF)
        } else {
            logger.info("use nio")
            serverChannel = NioServerSocketChannel::class.java
            clientChannel = NioSocketChannel::class.java
            parent = NioEventLoopGroup(parentThreads, parentTF)
            child = NioEventLoopGroup(childThreads, childTF)
        }
    }

    private fun threadFactory(name: String): ThreadFactory {
        return DefaultThreadFactory(name, true, Thread.NORM_PRIORITY, ThreadGroup(name))
    }

    fun serverChannel(): Class<out ServerSocketChannel> {
        return serverChannel
    }

    fun clientChannel(): Class<out SocketChannel> {
        return clientChannel
    }

    fun parent(): EventLoopGroup {
        return parent
    }

    fun child(): EventLoopGroup {
        return child
    }

    override fun doStart() {}

    override fun doStop() {}

    override fun doClose() {
        child.shutdownGracefully()
        parent.shutdownGracefully()
    }
}
