package org.mitallast.queue.rest.action.queue.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.stats.QueueStatsRequest;
import org.mitallast.queue.action.queue.stats.QueueStatsResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.rest.*;

import java.io.IOException;
import java.io.OutputStream;

public class RestQueueStatsAction extends BaseRestHandler {

    @Inject
    public RestQueueStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.GET, "/{queue}/_stats", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        QueueStatsRequest queueStatsRequest = new QueueStatsRequest();
        queueStatsRequest.setQueue(request.param("queue"));
        client.queue().queueStatsRequest(queueStatsRequest, new ActionListener<QueueStatsResponse>() {
            @Override
            public void onResponse(QueueStatsResponse queueStatsResponse) {
                JsonRestResponse restResponse = new JsonRestResponse(HttpResponseStatus.OK);
                JsonFactory factory = new JsonFactory();
                try (OutputStream stream = restResponse.getOutputStream()) {
                    QueueStats queueStats = queueStatsResponse.getStats();
                    JsonGenerator generator = factory.createGenerator(stream);
                    generator.writeStartObject();
                    generator.writeStringField("name", queueStats.getQueue().getName());
                    generator.writeNumberField("size", queueStats.getSize());
                    generator.writeEndObject();
                    generator.close();
                    session.sendResponse(restResponse);
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