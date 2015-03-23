package org.mitallast.queue.stomp;

import io.netty.channel.Channel;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.util.internal.ConcurrentSet;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.mitallast.queue.queue.Queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class StompSubscriptionController {
    private final ReentrantLock lock;
    private final ConcurrentMap<Queue, ConcurrentSet<Channel>> queueChannelMap;

    public StompSubscriptionController() {
        this.lock = new ReentrantLock();
        this.queueChannelMap = new ConcurrentHashMapV8<>();
    }

    public void onEvent(Queue queue, StompFrame stompFrame) {
        ConcurrentSet<Channel> channelSet = queueChannelMap.get(queue);
        if (channelSet != null) {
            final List<Channel> list = new ArrayList<>(channelSet.size());
            list.addAll(channelSet.stream().collect(Collectors.toList()));
            final Channel channel = list.get(stompFrame.hashCode() % list.size());
            channel.write(stompFrame, channel.voidPromise());
        }
    }

    public void subscribe(final Queue queue, final Channel channel) {
        lock.lock();
        try {
            ConcurrentSet<Channel> channelSet = queueChannelMap.get(queue);
            if (channelSet == null) {
                channelSet = new ConcurrentSet<>();
                queueChannelMap.put(queue, channelSet);
            }
            channelSet.add(channel);
        } finally {
            lock.unlock();
        }
    }

    public void unsubscribe(Queue queue, Channel channel) {
        lock.lock();
        try {
            ConcurrentSet<Channel> channelSet = queueChannelMap.get(queue);
            if (channelSet != null) {
                channelSet.remove(channel);
            }
        } finally {
            lock.unlock();
        }
    }

    public void unsubscribe(Channel channel) {
        lock.lock();
        try {
            for (ConcurrentSet<Channel> channelSet : queueChannelMap.values()) {
                channelSet.remove(channel);
            }
        } finally {
            lock.unlock();
        }
    }
}