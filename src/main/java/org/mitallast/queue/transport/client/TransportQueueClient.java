package org.mitallast.queue.transport.client;

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
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public class TransportQueueClient implements QueueClient {

    private final static ResponseMapper<EnQueueResponse> EN_QUEUE_RESPONSE_MAPPER = new ResponseMapper<>(EnQueueResponse::new);
    private final static ResponseMapper<DeQueueResponse> DE_QUEUE_RESPONSE_MAPPER = new ResponseMapper<>(DeQueueResponse::new);
    private final static ResponseMapper<QueueStatsResponse> QUEUE_STATS_RESPONSE_MAPPER = new ResponseMapper<>(QueueStatsResponse::new);
    private final static ResponseMapper<DeleteResponse> DELETE_RESPONSE_MAPPER = new ResponseMapper<>(DeleteResponse::new);
    private final static ResponseMapper<GetResponse> GET_RESPONSE_MAPPER = new ResponseMapper<>(GetResponse::new);
    private final static ResponseMapper<PeekQueueResponse> PEEK_QUEUE_RESPONSE_MAPPER = new ResponseMapper<>(PeekQueueResponse::new);

    private final TransportClient transportClient;

    public TransportQueueClient(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    public SmartFuture<EnQueueResponse> enqueueRequest(EnQueueRequest request) throws IOException {
        return transportClient.send(request, EN_QUEUE_RESPONSE_MAPPER);
    }

    @Override
    public void enqueueRequest(EnQueueRequest request, Listener<EnQueueResponse> listener) {
        transportClient.send(request, EN_QUEUE_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<DeQueueResponse> dequeueRequest(DeQueueRequest request) throws IOException {
        return transportClient.send(request, DE_QUEUE_RESPONSE_MAPPER);
    }

    @Override
    public void dequeueRequest(DeQueueRequest request, Listener<DeQueueResponse> listener) {
        transportClient.send(request, DE_QUEUE_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<QueueStatsResponse> queueStatsRequest(QueueStatsRequest request) throws IOException {
        return transportClient.send(request, QUEUE_STATS_RESPONSE_MAPPER);
    }

    @Override
    public void queueStatsRequest(QueueStatsRequest request, Listener<QueueStatsResponse> listener) {
        transportClient.send(request, QUEUE_STATS_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<DeleteResponse> deleteRequest(DeleteRequest request) throws IOException {
        return transportClient.send(request, DELETE_RESPONSE_MAPPER);
    }

    @Override
    public void deleteRequest(DeleteRequest request, Listener<DeleteResponse> listener) {
        transportClient.send(request, DELETE_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<GetResponse> getRequest(GetRequest request) throws IOException {
        return transportClient.send(request, GET_RESPONSE_MAPPER);
    }

    @Override
    public void getRequest(GetRequest request, Listener<GetResponse> listener) {
        transportClient.send(request, GET_RESPONSE_MAPPER).on(listener);
    }

    @Override
    public SmartFuture<PeekQueueResponse> peekQueueRequest(PeekQueueRequest request) throws IOException {
        return transportClient.send(request, PEEK_QUEUE_RESPONSE_MAPPER);
    }

    @Override
    public void peekQueueRequest(PeekQueueRequest request, Listener<PeekQueueResponse> listener) {
        transportClient.send(request, PEEK_QUEUE_RESPONSE_MAPPER).on(listener);
    }
}