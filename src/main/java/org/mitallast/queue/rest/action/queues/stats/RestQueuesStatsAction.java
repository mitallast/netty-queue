package org.mitallast.queue.rest.action.queues.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queues.stats.QueuesStatsRequest;
import org.mitallast.queue.action.queues.stats.QueuesStatsResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.*;

import java.io.IOException;
import java.io.OutputStream;

public class RestQueuesStatsAction extends BaseRestHandler {

    public RestQueuesStatsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.GET, "/_stats", this);
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        client.queues().queuesStatsRequest(new QueuesStatsRequest(), new ActionListener<QueuesStatsResponse>() {
            @Override
            public void onResponse(QueuesStatsResponse response) {
                JsonRestResponse restResponse = new JsonRestResponse(HttpResponseStatus.OK);
                JsonFactory factory = new JsonFactory();
                try (OutputStream stream = restResponse.getOutputStream()) {
                    JsonGenerator generator = factory.createGenerator(stream);
                    generator.writeStartObject();
                    generator.writeArrayFieldStart("queues");
                    for (String queue : response.getQueues()) {
                        generator.writeString(queue);
                    }
                    generator.writeEndArray();
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
