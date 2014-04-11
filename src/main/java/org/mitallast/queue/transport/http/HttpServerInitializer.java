package org.mitallast.queue.transport.http;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

    private final HttpServerHandler httpHandler;

    @Inject
    public HttpServerInitializer(HttpServerHandler httpHandler) {
        super();
        this.httpHandler = httpHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("decoder", new HttpRequestDecoder(4096, 8192, 8192, false));
        pipeline.addLast("aggregator", new HttpObjectAggregator(1048576));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("httpHandler", httpHandler);
    }
}