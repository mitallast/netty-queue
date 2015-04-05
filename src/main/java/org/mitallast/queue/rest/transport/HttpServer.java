package org.mitallast.queue.rest.transport;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.RestController;

public class HttpServer extends NettyServer {
    private RestController restController;

    @Inject
    public HttpServer(Settings settings, RestController restController) {
        super(settings, HttpServer.class, HttpServer.class);
        this.restController = restController;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new HttpServerInitializer(new HttpServerHandler(restController));
    }
}
