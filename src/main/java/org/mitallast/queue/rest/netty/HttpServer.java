package org.mitallast.queue.rest.netty;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.ChannelInitializer;
import org.mitallast.queue.common.netty.NettyProvider;
import org.mitallast.queue.common.netty.NettyServer;

public class HttpServer extends NettyServer {
    private final HttpServerHandler serverHandler;
    private final WebSocketFrameHandler webSocketFrameHandler;

    @Inject
    public HttpServer(Config config, NettyProvider provider, HttpServerHandler serverHandler, WebSocketFrameHandler webSocketFrameHandler) {
        super(config, provider,
            config.getString("rest.host"),
            config.getInt("rest.port")
        );
        this.serverHandler = serverHandler;
        this.webSocketFrameHandler = webSocketFrameHandler;
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new HttpServerInitializer(serverHandler, webSocketFrameHandler);
    }
}
