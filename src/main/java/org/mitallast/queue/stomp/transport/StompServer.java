package org.mitallast.queue.stomp.transport;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.settings.Settings;

public class StompServer extends NettyServer {

    private final StompServerHandler stompServerHandler;

    @Inject
    public StompServer(Settings settings, StompServerHandler stompServerHandler) {
        super(settings);
        this.stompServerHandler = stompServerHandler;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new StompServerInitializer(stompServerHandler);
    }
}
