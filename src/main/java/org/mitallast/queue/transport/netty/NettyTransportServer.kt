package org.mitallast.queue.transport.netty

import com.google.inject.Inject
import com.typesafe.config.Config
import io.netty.channel.*
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.common.logging.LoggingService
import org.mitallast.queue.common.netty.NettyProvider
import org.mitallast.queue.common.netty.NettyServer
import org.mitallast.queue.security.ECDHFlow
import org.mitallast.queue.security.ECDHRequest
import org.mitallast.queue.security.SecurityService
import org.mitallast.queue.transport.DiscoveryNode
import org.mitallast.queue.transport.TransportController
import org.mitallast.queue.transport.TransportServer

@Suppress("OverridingDeprecatedMember")
class NettyTransportServer @Inject constructor(
    config: Config,
    private val logging: LoggingService,
    provider: NettyProvider,
    private val transportController: TransportController,
    private val securityService: SecurityService
) : NettyServer(
    config,
    logging,
    provider,
    config.getString("transport.host"),
    config.getInt("transport.port")
), TransportServer {

    private val discoveryNode: DiscoveryNode = DiscoveryNode(host, port)

    override fun localNode(): DiscoveryNode {
        return discoveryNode
    }

    override fun channelInitializer(): ChannelInitializer<*> {
        return TransportServerInitializer()
    }

    private inner class TransportServerInitializer : ChannelInitializer<Channel>() {
        override fun initChannel(ch: Channel) {
            val pipeline = ch.pipeline()
            pipeline.addLast(CodecDecoder(logging))
            pipeline.addLast(CodecEncoder())
            pipeline.addLast(ECDHNewEncoder())
            pipeline.addLast(ECDHCodecDecoder())
            pipeline.addLast(TransportServerHandler())
        }
    }

    @ChannelHandler.Sharable
    private inner class TransportServerHandler : SimpleChannelInboundHandler<Message>(true) {
        override fun isSharable(): Boolean {
            return true
        }

        override fun channelRegistered(ctx: ChannelHandlerContext) {
            ctx.channel().attr(ECDHFlow.key).set(securityService.ecdh())
        }

        override fun channelRead0(ctx: ChannelHandlerContext, message: Message) {
            if (message is ECDHRequest) {
                logger.info("received ecdh request start")
                val ecdh = ctx.channel().attr(ECDHFlow.key).get()
                ecdh.keyAgreement(message)
                logger.info("send ecdh response start")
                ctx.writeAndFlush(ecdh.responseStart())
            } else {
                transportController.dispatch(message)
            }
        }

        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
            logger.error("unexpected channel error, close channel", cause)
            ctx.close()
        }
    }
}
