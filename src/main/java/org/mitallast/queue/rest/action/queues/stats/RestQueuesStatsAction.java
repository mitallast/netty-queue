package org.mitallast.queue.rest.action.queues.stats;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.JsonRestResponse;

import java.io.IOException;

public class RestQueuesStatsAction extends BaseRestHandler {

    @Inject
    public RestQueuesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.GET, "/_stats", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestSession session) {
        client.queues().queuesStatsRequest(new QueuesStatsRequest(), new ActionListener<QueuesStatsResponse>() {
            @Override
            public void onResponse(QueuesStatsResponse response) {
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
                    session.sendResponse(new JsonRestResponse(HttpResponseStatus.OK, buffer));
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
