package org.mitallast.queue.common.netty

import com.typesafe.config.Config
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.*
import org.mitallast.queue.common.component.AbstractLifecycleComponent
import org.mitallast.queue.transport.DiscoveryNode

import java.util.concurrent.TimeUnit

abstract class NettyClientBootstrap protected constructor(config: Config, protected val provider: NettyProvider) : AbstractLifecycleComponent() {
    protected val maxContentLength = config.getInt("netty.max_content_length")
    private val keepAlive = config.getBoolean("netty.keep_alive")
    private val reuseAddress = config.getBoolean("netty.reuse_address")
    private val tcpNoDelay = config.getBoolean("netty.tcp_no_delay")
    private val sndBuf = config.getInt("netty.snd_buf")
    private val rcvBuf = config.getInt("netty.rcv_buf")
    private val connectTimeout = config.getDuration("netty.connect_timeout", TimeUnit.MILLISECONDS).toInt()
    @Volatile private var bootstrap: Bootstrap? = null

    override fun doStart() {
        bootstrap = Bootstrap()
            .channel(provider.clientChannel())
            .group(provider.child())
            .option(ChannelOption.SO_REUSEADDR, reuseAddress)
            .option(ChannelOption.SO_KEEPALIVE, keepAlive)
            .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
            .option(ChannelOption.SO_SNDBUF, sndBuf)
            .option(ChannelOption.SO_RCVBUF, rcvBuf)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout)
            .option<ByteBufAllocator>(ChannelOption.ALLOCATOR, PooledByteBufAllocator(true))
            .option<RecvByteBufAllocator>(ChannelOption.RCVBUF_ALLOCATOR, FixedRecvByteBufAllocator(65536))
            .handler(channelInitializer())
    }

    protected abstract fun channelInitializer(): ChannelInitializer<*>

    fun connect(node: DiscoveryNode): ChannelFuture {
        checkIsStarted()
        return bootstrap!!.connect(node.host, node.port)
    }

    fun connect(host: String, port: Int): ChannelFuture {
        checkIsStarted()
        return bootstrap!!.connect(host, port)
    }

    override fun doStop() {}

    override fun doClose() {

    }
}
