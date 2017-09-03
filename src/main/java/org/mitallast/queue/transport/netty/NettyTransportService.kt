package org.mitallast.queue.transport.netty

import com.google.common.base.Preconditions
import com.google.inject.Inject
import com.typesafe.config.Config
import io.netty.channel.*
import javaslang.collection.HashMap
import javaslang.collection.Map
import org.mitallast.queue.common.codec.Message
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
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

@Suppress("OverridingDeprecatedMember")
class NettyTransportService @Inject constructor(
    config: Config,
    provider: NettyProvider,
    private val transportController: TransportController,
    private val securityService: SecurityService
) : NettyClientBootstrap(config, provider), TransportService {
    private val connectionLock = ReentrantLock()
    private val maxConnections = config.getInt("transport.max_connections")
    @Volatile private var connectedNodes: Map<DiscoveryNode, NodeChannel> = HashMap.empty()

    override fun channelInitializer(): ChannelInitializer<Channel> {
        return object : ChannelInitializer<Channel>() {
            override fun initChannel(ch: Channel) {
                val pipeline = ch.pipeline()
                pipeline.addLast(CodecDecoder())
                pipeline.addLast(CodecEncoder())
                pipeline.addLast(ECDHCodecEncoder())
                pipeline.addLast(ECDHCodecDecoder())
                pipeline.addLast(object : SimpleChannelInboundHandler<Message>(false) {

                    override fun channelRegistered(ctx: ChannelHandlerContext) {
                        logger.info("start ecdh")
                        ctx.channel().attr(ECDHFlow.key).set(securityService.ecdh())
                    }

                    override fun channelActive(ctx: ChannelHandlerContext) {
                        logger.info("send ecdh request start")
                        val ecdh = ctx.channel().attr(ECDHFlow.key).get()
                        ctx.writeAndFlush(ecdh.requestStart())
                        super.channelActive(ctx)
                    }

                    override fun channelRead0(ctx: ChannelHandlerContext, message: Message) {
                        val ecdh = ctx.channel().attr(ECDHFlow.key).get()
                        if (message is ECDHResponse) {
                            logger.info("received response ecdh start")
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
        return connectedNodes.getOrElse(node, null) ?: throw IllegalArgumentException("Not connected to node: " + node)
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
        private val reconnectScheduled = AtomicBoolean()
        private val closed = AtomicBoolean(false)
        private val channels: Array<Channel?> = arrayOfNulls(maxConnections)

        @Synchronized
        fun open() {
            logger.info("connect to {}", node)
            val channelFutures: Array<ChannelFuture?> = arrayOfNulls(maxConnections)
            for (i in 0 until maxConnections) {
                channelFutures[i] = connect(node)
            }
            logger.debug("await channel open {}", node)
            for (i in 0 until maxConnections) {
                try {
                    val channel = channelFutures[i]
                        ?.awaitUninterruptibly()
                        ?.channel()
                    channels[i] = channel
                } catch (e: Throwable) {
                    logger.error("error connect to {}", node, e)
                    if (reconnectScheduled.compareAndSet(false, true)) {
                        provider.child().execute { this.reconnect() }
                    }
                }

            }
        }

        @Synchronized private fun reconnect() {
            if (closed.get()) {
                return
            }
            logger.warn("reconnect to {}", node)
            channels.indices
                .filter { channels[it] == null || !channels[it]!!.isOpen }
                .forEach {
                    try {
                        val channel = connect(node)
                            .awaitUninterruptibly()
                            .channel()
                        channels[it] = channel
                    } catch (e: Throwable) {
                        logger.error("error reconnect to {}", node, e)
                    }
                }
            reconnectScheduled.set(false)
        }

        override fun send(message: Message) {
            val channel = channel()
            val ecdh = channel.attr(ECDHFlow.key).get()
            if (ecdh.isAgreement) {
                channel.writeAndFlush(message, channel.voidPromise())
            } else {
                ecdh.agreementFuture().whenComplete { _, _ -> channel.writeAndFlush(message, channel.voidPromise()) }
            }
        }

        @Synchronized override fun close() {
            closed.set(true)
            for (channel in channels) {
                channel?.close()
            }
        }

        private fun channel(): Channel {
            var index = channelCounter.get().toInt() % channels.size
            channelCounter.set((index + 1).toLong())
            val loopIndex = index
            do {
                if (channels[index] != null && channels[index]!!.isOpen) {
                    return channels[index]!!
                } else if (reconnectScheduled.compareAndSet(false, true)) {
                    provider.child().execute { this.reconnect() }
                }
                index = (index + 1) % channels.size
            } while (index != loopIndex)
            throw RuntimeException("error connect to " + node)
        }
    }
}
