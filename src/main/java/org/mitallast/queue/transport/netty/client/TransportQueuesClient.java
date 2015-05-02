package org.mitallast.queue.transport.netty.client;

import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.client.QueuesClient;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.transport.TransportClient;
import org.mitallast.queue.transport.netty.ResponseMapper;

public class TransportQueuesClient implements QueuesClient {

    private final static ResponseMapper<QueuesStatsResponse> QUEUES_STATS_RESPONSE_MAPPER = new ResponseMapper<>(QueuesStatsResponse::new);
    private final static ResponseMapper<CreateQueueResponse> CREATE_QUEUE_RESPONSE_MAPPER = new ResponseMapper<>(CreateQueueResponse::new);
    private final static ResponseMapper<DeleteQueueResponse> DELETE_QUEUE_RESPONSE_MAPPER = new ResponseMapper<>(DeleteQueueResponse::new);

    private final TransportClient transportClient;

    public TransportQueuesClient(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    public SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request) {
        return transportClient.send(request, QUEUES_STATS_RESPONSE_MAPPER);
    }

    @Override
    public SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request) {
        return transportClient.send(request, CREATE_QUEUE_RESPONSE_MAPPER);
    }

    @Override
    public SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request) {
        return transportClient.send(request, DELETE_QUEUE_RESPONSE_MAPPER);
    }
}
