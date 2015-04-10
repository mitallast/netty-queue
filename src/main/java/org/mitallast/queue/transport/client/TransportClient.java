package org.mitallast.queue.transport.client;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.AttributeKey;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.client.base.Client;
import org.mitallast.queue.client.base.QueueClient;
import org.mitallast.queue.client.base.QueuesClient;
import org.mitallast.queue.common.concurrent.futures.Futures;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.netty.NettyClient;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.ByteBufStreamOutput;
import org.mitallast.queue.transport.TransportModule;
import org.mitallast.queue.transport.transport.TransportFrame;
import org.mitallast.queue.transport.transport.TransportFrameDecoder;
import org.mitallast.queue.transport.transport.TransportFrameEncoder;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransportClient extends NettyClient implements Client {

    private final static AttributeKey<ConcurrentMap<Long, SmartFuture<TransportFrame>>> attr = AttributeKey.valueOf("queue");
    private final AtomicLong requestCounter = new AtomicLong();

    private final TransportQueueClient queueClient;
    private final TransportQueuesClient queuesClient;
    private final AtomicLong flushCount = new AtomicLong();

    @Inject
    public TransportClient(Settings settings) {
        super(settings, TransportClient.class, TransportModule.class);
        queueClient = new TransportQueueClient(this);
        queuesClient = new TransportQueuesClient(this);
    }

    @Override
    protected int defaultPort() {
        return 10080;
    }

    @Override
    protected void init() {
        channel.attr(attr).set(new ConcurrentHashMap<>());
    }

    public SmartFuture<TransportFrame> send(TransportFrame frame) {
        final SmartFuture<TransportFrame> future = Futures.future();
        final Channel localChannel = channel;
        localChannel.attr(attr).get().put(frame.getRequest(), future);
        flushCount.incrementAndGet();
        localChannel.write(frame, localChannel.voidPromise());
        // automatic flush
        localChannel.pipeline().lastContext().executor().execute(() -> {
            if (flushCount.decrementAndGet() == 0) {
                logger.info("flush");
                localChannel.flush();
            }
        });
        return future;
    }

    public <Request extends ActionRequest, Response extends ActionResponse>
    SmartFuture<Response> send(Request request, ResponseMapper<Response> mapper) {
        ByteBuf buffer = channel.alloc().heapBuffer();
        try (ByteBufStreamOutput streamOutput = new ByteBufStreamOutput(buffer)) {
            streamOutput.writeInt(request.actionType().id());
            request.writeTo(streamOutput);
        } catch (IOException e) {
            logger.error("error write", e);
            return Futures.future(e);
        }
        TransportFrame requestFrame = TransportFrame.of(requestCounter.incrementAndGet(), buffer);
        return send(requestFrame).map(mapper);
    }

    @Override
    protected ChannelInitializer channelInitializer() {
        return new ChannelInitializer() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new TransportFrameDecoder());
                pipeline.addLast(new TransportFrameEncoder());
                pipeline.addLast(new SimpleChannelInboundHandler<TransportFrame>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, TransportFrame response) throws Exception {
                        SmartFuture<TransportFrame> future = ctx.attr(attr).get().remove(response.getRequest());
                        if (future == null) {
                            logger.warn("future not found");
                        } else {
                            future.invoke(response);
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        logger.error("unexpected exception {}", ctx, cause);
                    }
                });
            }
        };
    }

    @Override
    public QueuesClient queues() {
        return queuesClient;
    }

    @Override
    public QueueClient queue() {
        return queueClient;
    }
}
