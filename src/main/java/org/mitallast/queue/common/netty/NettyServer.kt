package org.mitallast.queue.common.netty

import com.typesafe.config.Config
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.*
import org.mitallast.queue.common.component.AbstractLifecycleComponent

abstract class NettyServer protected constructor(config: Config, protected val provider: NettyProvider, protected val host: String, protected val port: Int) : AbstractLifecycleComponent() {
    private val backlog: Int = config.getInt("netty.backlog")
    private val keepAlive: Boolean = config.getBoolean("netty.keep_alive")
    private val reuseAddress: Boolean = config.getBoolean("netty.reuse_address")
    private val tcpNoDelay: Boolean = config.getBoolean("netty.tcp_no_delay")
    private val sndBuf: Int = config.getInt("netty.snd_buf")
    private val rcvBuf: Int = config.getInt("netty.rcv_buf")
    protected var channel: Channel? = null
    protected var bootstrap: ServerBootstrap? = null

    override fun doStart() {
        try {
            bootstrap = ServerBootstrap()
            bootstrap!!.group(provider.parent(), provider.child())
                .channel(provider.serverChannel())
                .childHandler(channelInitializer())
                .option(ChannelOption.SO_BACKLOG, backlog)
                .option(ChannelOption.SO_REUSEADDR, reuseAddress)
                .option(ChannelOption.SO_KEEPALIVE, keepAlive)
                .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
                .option(ChannelOption.SO_SNDBUF, sndBuf)
                .option(ChannelOption.SO_RCVBUF, rcvBuf)
                .option<ByteBufAllocator>(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option<RecvByteBufAllocator>(ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator())

            logger.info("listen {}:{}", host, port)
            channel = bootstrap!!.bind(host, port)
                .sync()
                .channel()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw RuntimeException(e)
        }

    }

    protected abstract fun channelInitializer(): ChannelInitializer<*>

    override fun doStop() {}

    override fun doClose() {
        try {
            if (channel != null) {
                channel!!.close().sync()
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw RuntimeException(e)
        }

        channel = null
        bootstrap = null
    }
}
