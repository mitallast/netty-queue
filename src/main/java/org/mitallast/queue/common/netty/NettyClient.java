package org.mitallast.queue.common.netty;

import com.typesafe.config.Config;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public abstract class NettyClient extends NettyClientBootstrap {

    protected final String host;
    protected final int port;
    protected volatile Channel channel;

    protected NettyClient(Config config, NettyProvider provider, String host, int port) {
        super(config, provider);
        this.host = host;
        this.port = port;
    }

    @Override
    protected void doStart() {
        super.doStart();
        try {
            channel = connect(host, port).sync().channel();
            init();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void init() {
    }

    @Override
    protected void doStop() {
        channel.close().awaitUninterruptibly();
        super.doStop();
    }

    protected abstract ChannelInitializer channelInitializer();
}
