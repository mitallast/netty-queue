package org.mitallast.queue.rest.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

    private final HttpServerHandler httpHandler;
    private final WebSocketFrameHandler webSocketFrameHandler;

    public HttpServerInitializer(HttpServerHandler httpHandler, WebSocketFrameHandler webSocketFrameHandler) {
        super();
        this.httpHandler = httpHandler;
        this.webSocketFrameHandler = webSocketFrameHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new HttpServerCodec(4096, 8192, 8192, false));
        pipeline.addLast(new HttpObjectAggregator(65536));
        pipeline.addLast(new ChunkedWriteHandler());
//        pipeline.addLast(new WebSocketServerCompressionHandler());
//        pipeline.addLast(new WebSocketServerProtocolHandler("/ws/", null, true));
//        pipeline.addLast(webSocketFrameHandler);
        pipeline.addLast(httpHandler);
    }
}