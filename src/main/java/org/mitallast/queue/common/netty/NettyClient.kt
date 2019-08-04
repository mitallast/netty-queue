package org.mitallast.queue.common.netty

import com.typesafe.config.Config
import io.netty.channel.Channel
import io.netty.channel.ChannelInitializer
import org.mitallast.queue.common.logging.LoggingService

abstract class NettyClient protected constructor(
    config: Config,
    logging: LoggingService,
    provider: NettyProvider,
    protected val host: String,
    protected val port: Int
) : NettyClientBootstrap(config, logging, provider) {
    @Volatile
    protected var channel: Channel? = null

    init {
        this.channel = null
    }

    override fun doStart() {
        super.doStart()
        try {
            channel = connect(host, port).sync().channel()
            init()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }

    }

    protected fun init() {}

    override fun doStop() {
        if (channel != null) {
            channel!!.close().awaitUninterruptibly()
            channel = null
        }
        super.doStop()
    }

    abstract override fun channelInitializer(): ChannelInitializer<*>
}
