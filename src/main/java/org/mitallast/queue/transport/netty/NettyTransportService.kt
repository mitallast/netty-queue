package org.mitallast.queue.transport.netty

import com.google.common.base.Preconditions
import com.google.inject.Inject
import com.typesafe.config.Config
import io.netty.channel.*
import io.netty.util.AttributeKey
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.GenericFutureListener
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.logging.LoggingService
import org.mitallast.queue.common.netty.NettyClientBootstrap
import org.mitallast.queue.common.netty.NettyProvider
import org.mitallast.queue.security.ECDHFlow
import org.mitallast.queue.security.ECDHResponse
import org.mitallast.queue.security.SecurityService
import org.mitallast.queue.transport.DiscoveryNode
import org.mitallast.queue.transport.TransportChannel
import org.mitallast.queue.transport.TransportController
import org.mitallast.queue.transport.TransportService
import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

@Suppress("OverridingDeprecatedMember")
class NettyTransportService @Inject constructor(
    config: Config,
    private val logging: LoggingService,
    provider: NettyProvider,
    private val transportController: TransportController,
    private val securityService: SecurityService
) : NettyClientBootstrap(config, logging, provider), TransportService {
    private val connectionLock = ReentrantLock()
    private val maxConnections = config.getInt("transport.max_connections")
    @Volatile
    private var connectedNodes: Map<DiscoveryNode, NodeChannel> = HashMap.empty()

    override fun channelInitializer(): ChannelInitializer<Channel> {
        return object : ChannelInitializer<Channel>() {
            override fun initChannel(ch: Channel) {
                ch.attr(flushKey).set(FlushListener(ch))
                val pipeline = ch.pipeline()
                pipeline.addLast(CodecEncoder())
                pipeline.addLast(CodecDecoder(logging))
                pipeline.addLast(ECDHNewEncoder())
                pipeline.addLast(ECDHCodecDecoder())
                pipeline.addLast(object : SimpleChannelInboundHandler<Message>(false) {

                    override fun channelRegistered(ctx: ChannelHandlerContext) {
                        logger.trace("start ecdh")
                        ctx.channel().attr(ECDHFlow.key).set(securityService.ecdh())
                    }

                    override fun channelActive(ctx: ChannelHandlerContext) {
                        logger.trace("send ecdh request start")
                        val ecdh = ctx.channel().attr(ECDHFlow.key).get()
                        ctx.writeAndFlush(ecdh.requestStart())
                        super.channelActive(ctx)
                    }

                    override fun channelRead0(ctx: ChannelHandlerContext, message: Message) {
                        val ecdh = ctx.channel().attr(ECDHFlow.key).get()
                        if (message is ECDHResponse) {
                            logger.trace("received response ecdh start")
                            ecdh.keyAgreement(message)
                        } else {
                            transportController.dispatch(message)
                        }
                    }

                    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
                        logger.error("unexpected exception {}", ctx, cause)
                        ctx.close()
                    }
                })
            }
        }
    }

    override fun doStop() {
        connectedNodes.keySet().forEach { disconnectFromNode(it) }
        super.doStop()
    }

    override fun connectToNode(node: DiscoveryNode) {
        checkIsStarted()
        Preconditions.checkNotNull(node)
        if (connectedNodes.getOrElse(node, null) != null) {
            return
        }
        connectionLock.lock()
        try {
            if (connectedNodes.getOrElse(node, null) != null) {
                return
            }
            val nodeChannel = NodeChannel(node)
            connectedNodes = connectedNodes.put(node, nodeChannel)
            nodeChannel.open()
            logger.info("connected to node {}", node)
        } finally {
            connectionLock.unlock()
        }
    }

    override fun disconnectFromNode(node: DiscoveryNode) {
        Preconditions.checkNotNull(node)
        connectionLock.lock()
        try {
            val nodeChannel = connectedNodes.getOrElse(node, null) ?: return
            logger.info("disconnect from node {}", node)
            nodeChannel.close()
            connectedNodes = connectedNodes.remove(node)
        } finally {
            connectionLock.unlock()
        }
    }

    private fun channel(node: DiscoveryNode): TransportChannel {
        Preconditions.checkNotNull(node)
        return connectedNodes.getOrElse(node, null) ?: throw IllegalArgumentException("Not connected to node: $node")
    }

    override fun send(node: DiscoveryNode, message: Message) {
        try {
            connectToNode(node)
            channel(node).send(message)
        } catch (e: Exception) {
            logger.error("error send message", e)
        }
    }

    private inner class NodeChannel constructor(private val node: DiscoveryNode) : TransportChannel, Closeable {
        private val channelCounter = AtomicLong()
        private val channelFutures: Array<ChannelFuture?> = arrayOfNulls(maxConnections)
        private val lock = Object()
        private val closed = AtomicBoolean(false)

        fun open() {
            logger.info("connect to {}", node)
            for (i in 0 until maxConnections) {
                connect(i)
            }
        }

        private fun connect(index: Int) {
            synchronized(lock) {
                val future: ChannelFuture = connect(node)
                future.addListener { result ->
                    synchronized(lock) {
                        if (result.isSuccess) {
                            val channel = future.channel()
                            channel.closeFuture().addListener {
                                if (!closed.get()) {
                                    connect(index)
                                }
                            }
                        } else {
                            connect(index)
                        }
                    }
                }
                channelFutures[index] = future
            }
        }

        override fun send(message: Message) {
            if (closed.get()) return
            var index = channelCounter.get().toInt() % maxConnections
            channelCounter.set((index + 1).toLong())
            var loopIndex = index
            do {
                val future = channelFutures[index]
                if (future != null && future.isSuccess) {
                    send(future.channel(), message)
                    return
                }
                index = (index + 1) % maxConnections
            } while (index != loopIndex)
            loopIndex = index
            do {
                val future = channelFutures[index]
                if (future != null && !future.isCancelled) {
                    future.addListener {
                        if (it.isSuccess) {
                            send(future.channel(), message)
                        } else {
                            send(message)
                        }
                    }
                    return
                }
                index = (index + 1) % maxConnections
            } while (index != loopIndex)
            logger.warn("error send message to {}", node)
        }

        private fun send(channel: Channel, message: Message) {
            val ecdh = channel.attr(ECDHFlow.key).get()
            val flush = channel.attr(flushKey).get()
            if (ecdh.isAgreement) {
                flush.increment()
                channel.write(message, channel.voidPromise())
                channel.eventLoop().execute(flush)
            } else {
                ecdh.agreementFuture().whenComplete { _, t ->
                    if (t == null) {
                        flush.increment()
                        channel.write(message, channel.voidPromise())
                        channel.eventLoop().execute(flush)
                    }
                }
            }
        }

        override fun close() {
            synchronized(lock) {
                closed.set(true)
                channelFutures.forEach { future ->
                    future?.addListener { future.channel().close() }
                }
            }
        }
    }

    private inner class FlushListener(val channel: Channel) : Runnable {
        override fun run() {
            val c = counter.decrementAndGet()
            if (c == 0) {
                channel.flush()
            }
        }

        private val counter = AtomicInteger()

        fun increment() {
            counter.incrementAndGet()
        }
    }

    companion object {
        private val flushKey: AttributeKey<FlushListener> = AttributeKey.valueOf("flush")
    }
}
