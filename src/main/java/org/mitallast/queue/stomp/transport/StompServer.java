package org.mitallast.queue.stomp.transport;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.stomp.StompController;

public class StompServer extends NettyServer {

    private final StompController stompController;

    @Inject
    public StompServer(Settings settings, StompController stompController) {
        super(settings);
        this.stompController = stompController;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new StompServerInitializer(new StompServerHandler(stompController));
    }
}
