package org.mitallast.queue.stomp.transport;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.stomp.StompSubframeAggregator;
import io.netty.handler.codec.stomp.StompSubframeDecoder;
import io.netty.handler.codec.stomp.StompSubframeEncoder;

public class StompServerInitializer extends ChannelInitializer<SocketChannel> {

    private final StompServerHandler stompHandler;

    @Inject
    public StompServerInitializer(StompServerHandler stompHandler) {
        super();
        this.stompHandler = stompHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("decoder", new StompSubframeDecoder(4096, 8192));
        pipeline.addLast("aggregator", new StompSubframeAggregator(1048576));
        pipeline.addLast("encoder", new StompSubframeEncoder());
        pipeline.addLast("handler", stompHandler);
    }
}
