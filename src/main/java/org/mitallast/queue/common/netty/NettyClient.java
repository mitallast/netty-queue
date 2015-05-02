package org.mitallast.queue.common.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.settings.Settings;

public abstract class NettyClient extends NettyClientBootstrap {

    protected final String host;
    protected final int port;
    protected volatile Channel channel;

    public NettyClient(Settings settings, Class loggerClass, Class componentClass) {
        super(settings, loggerClass, componentClass);
        host = componentSettings.get("host", "127.0.0.1");
        port = componentSettings.getAsInt("port", defaultPort());
    }

    protected int defaultPort() {
        return 9080;
    }

    @Override
    protected void doStart() throws QueueException {
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
    protected void doStop() throws QueueException {
        channel.close().awaitUninterruptibly();
        super.doStop();
    }

    public final void flush() {
        channel.flush();
    }

    protected abstract ChannelInitializer channelInitializer();
}
