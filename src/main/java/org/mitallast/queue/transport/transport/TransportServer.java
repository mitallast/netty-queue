package org.mitallast.queue.transport.transport;

import com.google.inject.Inject;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.mitallast.queue.common.netty.NettyServer;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.TransportModule;

public class TransportServer extends NettyServer {

    private final TransportController transportController;

    @Inject
    public TransportServer(Settings settings, TransportController transportController) {
        super(settings, TransportServer.class, TransportModule.class);
        this.transportController = transportController;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new TransportServerInitializer(new TransportServerHandler(transportController));
    }

    protected int defaultPort() {
        return 10080;
    }
}
