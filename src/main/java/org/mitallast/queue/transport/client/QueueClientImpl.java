package org.mitallast.queue.transport.client;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import org.mitallast.queue.action.queue.delete.DeleteRequest;
import org.mitallast.queue.action.queue.delete.DeleteResponse;
import org.mitallast.queue.action.queue.dequeue.DeQueueRequest;
import org.mitallast.queue.action.queue.dequeue.DeQueueResponse;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.action.queue.get.GetRequest;
import org.mitallast.queue.action.queue.get.GetResponse;
import org.mitallast.queue.action.queue.peek.PeekQueueRequest;
import org.mitallast.queue.action.queue.peek.PeekQueueResponse;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.client.base.QueueClient;
import org.mitallast.queue.common.concurrent.futures.Futures;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.stream.ByteBufStreamOutput;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.transport.transport.TransportFrame;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class QueueClientImpl implements QueueClient {

    private final TransportClient transportClient;
    private final AtomicLong requestCounter = new AtomicLong();

    @Inject
    public QueueClientImpl(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    public SmartFuture<EnQueueResponse> enqueueRequest(EnQueueRequest request) throws IOException {
        ByteBuf buffer = transportClient.alloc().heapBuffer();
        request.writeTo(new ByteBufStreamOutput(buffer));
        TransportFrame requestFrame = TransportFrame.of(requestCounter.incrementAndGet(), buffer);
        SmartFuture<EnQueueResponse> future = Futures.future();
        transportClient.send(requestFrame).on(result -> {
            if (result.isValuableDone()) {
                TransportFrame response = result.getOrNull();
                if (response != null) {
                    EnQueueResponse enQueueResponse = null;
                    try (StreamInput streamInput = response.inputStream()) {
                        enQueueResponse = new EnQueueResponse();
                        enQueueResponse.readFrom(streamInput);
                    } catch (IOException e) {
                        future.invokeException(e);
                        return;
                    }
                    future.invoke(enQueueResponse);
                } else {
                    future.invokeException(new IllegalArgumentException());
                }
            } else {
                future.invokeException(result.getError());
            }
        });
        return future;
    }

    @Override
    public SmartFuture<DeQueueResponse> dequeueRequest(DeQueueRequest request) {
        return null;
    }

    @Override
    public SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) {
        return null;
    }

    @Override
    public SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request) {
        return null;
    }

    @Override
    public SmartFuture<GetResponse> getRequest(GetRequest request) {
        return null;
    }

    @Override
    public SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) {
        return null;
    }
}