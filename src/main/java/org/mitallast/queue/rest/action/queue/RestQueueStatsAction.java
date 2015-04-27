package org.mitallast.queue.rest.action.queue;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.client.base.Client;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

import java.io.IOException;

public class RestQueueStatsAction extends BaseRestHandler {

    @Inject
    public RestQueueStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.GET, "/{queue}/_stats", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        QueueStatsRequest queueStatsRequest = new QueueStatsRequest();
        queueStatsRequest.setQueue(request.param("queue").toString());
        client.queue().queueStatsRequest(queueStatsRequest, new Listener<QueueStatsResponse>() {
            @Override
            public void onResponse(QueueStatsResponse queueStatsResponse) {
                QueueStats queueStats = queueStatsResponse.getStats();
                ByteBuf buffer = Unpooled.buffer();
                try {
                    try (XStreamBuilder builder = createBuilder(request, buffer)) {
                        builder.writeStartObject();
                        builder.writeStringField("name", queueStats.getQueue().getName());
                        builder.writeNumberField("size", queueStats.getSize());
                        builder.writeEndObject();
                    }
                    session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
                } catch (IOException e) {
                    session.sendResponse(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                session.sendResponse(e);
            }
        });
    }
}
