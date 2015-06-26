package org.mitallast.queue.rest.action.queues;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

import java.io.IOException;

public class RestQueuesStatsAction extends BaseRestHandler {

    @Inject
    public RestQueuesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.GET, "/_stats", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        client.queues().queuesStatsRequest(QueuesStatsRequest.builder().build()).whenComplete((response, error) -> {
            if (error == null) {
                ByteBuf buffer = Unpooled.buffer();
                try {
                    try (XStreamBuilder builder = createBuilder(request, buffer)) {
                        builder.writeStartObject();
                        builder.writeArrayFieldStart("queues");
                        for (QueueStats queueStats : response.stats().getQueueStats()) {
                            builder.writeStartObject();
                            builder.writeStringField("name", queueStats.getQueue().getName());
                            builder.writeNumberField("size", queueStats.getSize());
                            builder.writeEndObject();
                        }
                        builder.writeEndArray();
                    }
                    session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
                } catch (IOException e) {
                    session.sendResponse(e);
                }
            } else {
                session.sendResponse(error);
            }
        });
    }
}
