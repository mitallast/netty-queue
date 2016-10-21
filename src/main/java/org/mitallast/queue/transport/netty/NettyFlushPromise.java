package org.mitallast.queue.transport.netty;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

class NettyFlushPromise implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger("flush");
    final static AttributeKey<NettyFlushPromise> attr = AttributeKey.valueOf("flush");

    private final Channel channel;
    private final AtomicLong counter;

    public NettyFlushPromise(Channel channel) {
        this.channel = channel;
        this.counter = new AtomicLong();
    }

    public void increment() {
        counter.incrementAndGet();
    }

    @Override
    public void run() {
        if (counter.decrementAndGet() == 0) {
            // logger.info("flush");
            channel.flush();
        }
    }
}
