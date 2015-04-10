package org.mitallast.queue.transport.client;

import org.mitallast.queue.action.queues.create.CreateQueueRequest;
import org.mitallast.queue.action.queues.create.CreateQueueResponse;
import org.mitallast.queue.action.queues.delete.DeleteQueueRequest;
import org.mitallast.queue.action.queues.delete.DeleteQueueResponse;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.client.base.QueuesClient;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.IOException;

public class TransportQueuesClient implements QueuesClient {

    private final static ResponseMapper<QueuesStatsResponse> QUEUES_STATS_RESPONSE_MAPPER = new ResponseMapper<>(QueuesStatsResponse::new);
    private final static ResponseMapper<CreateQueueResponse> CREATE_QUEUE_RESPONSE_MAPPER = new ResponseMapper<>(CreateQueueResponse::new);
    private final static ResponseMapper<DeleteQueueResponse> DELETE_QUEUE_RESPONSE_MAPPER = new ResponseMapper<>(DeleteQueueResponse::new);

    private final TransportClient transportClient;

    public TransportQueuesClient(TransportClient transportClient) {
        this.transportClient = transportClient;
    }

    @Override
    public SmartFuture<QueuesStatsResponse> queuesStatsRequest(QueuesStatsRequest request) throws IOException {
        return transportClient.send(request, QUEUES_STATS_RESPONSE_MAPPER);
    }

    @Override
    public SmartFuture<CreateQueueResponse> createQueue(CreateQueueRequest request) throws IOException {
        return transportClient.send(request, CREATE_QUEUE_RESPONSE_MAPPER);
    }

    @Override
    public SmartFuture<DeleteQueueResponse> deleteQueue(DeleteQueueRequest request) throws IOException {
        return transportClient.send(request, DELETE_QUEUE_RESPONSE_MAPPER);
    }
}
