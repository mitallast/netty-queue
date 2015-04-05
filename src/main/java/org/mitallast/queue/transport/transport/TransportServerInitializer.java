package org.mitallast.queue.transport.transport;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

public class TransportServerInitializer extends ChannelInitializer<SocketChannel> {

    private final TransportServerHandler transportServerHandler;

    public TransportServerInitializer(TransportServerHandler transportServerHandler) {

        this.transportServerHandler = transportServerHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new TransportFrameDecoder());
        pipeline.addLast(new TransportFrameEncoder());
        pipeline.addLast(transportServerHandler);
    }
}
