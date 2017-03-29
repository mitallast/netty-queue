package org.mitallast.queue.common.netty;

import com.typesafe.config.Config;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

import java.io.IOException;

public abstract class NettyClient extends NettyClientBootstrap {

    protected final String host;
    protected final int port;
    protected volatile Channel channel;

    public NettyClient(Config config) {
        super(config);
        host = config.getString("host");
        port = config.getInt("port");
    }

    @Override
    protected void doStart() throws IOException {
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
    protected void doStop() throws IOException {
        channel.close().awaitUninterruptibly();
        super.doStop();
    }

    protected abstract ChannelInitializer channelInitializer();
}
