package org.mitallast.queue.rest.action.raft;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.raft.resource.ResourceService;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

import java.io.IOException;

public class RestGetResourcesAction extends BaseRestHandler {

    private final ResourceService resourceService;

    @Inject
    public RestGetResourcesAction(Settings settings, RestController controller, ResourceService resourceService) {
        super(settings);
        this.resourceService = resourceService;
        controller.registerHandler(HttpMethod.GET, "/_raft/resource", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        String path = request.hasParam("path") ? request.param("path").toString() : "/";

        resourceService.children(path).whenComplete((resources, error) -> {
            if (error == null) {
                ByteBuf buffer = Unpooled.buffer();
                try (XStreamBuilder builder = createBuilder(request, buffer)) {
                    builder.writeStartObject();
                    builder.writeFieldName("resources");
                    builder.writeStartArray();
                    for (String resource : resources) {
                        builder.writeString(resource);
                    }
                    builder.writeEndArray();
                    builder.writeEndObject();
                } catch (IOException e) {
                    session.sendResponse(e);
                    return;
                }

                session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
            } else {
                session.sendResponse(error);
            }
        });
    }
}
