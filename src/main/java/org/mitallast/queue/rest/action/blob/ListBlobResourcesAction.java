package org.mitallast.queue.rest.action.blob;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.blob.DistributedStorageFSM;
import org.mitallast.queue.blob.DistributedStorageService;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Map;

public class ListBlobResourcesAction extends BaseRestHandler {

    private final DistributedStorageFSM fsm;

    @Inject
    public ListBlobResourcesAction(Config config, RestController controller, DistributedStorageFSM fsm) {
        super(config.getConfig("rest"), PutBlobResourceAction.class);
        this.fsm = fsm;

        controller.registerHandler(HttpMethod.GET, "/_blob", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        request.content().release();
        ImmutableMap<String, ImmutableList<DiscoveryNode>> routingMap = fsm.getRoutingMap().getRoutingMap();

        ByteBuf buffer = session.alloc().directBuffer();
        try {
            try (XStreamBuilder builder = createBuilder(request, buffer)) {
                builder.writeStartObject();
                builder.writeArrayFieldStart("resources");
                for (Map.Entry<String, ImmutableList<DiscoveryNode>> entry : routingMap.entrySet()) {
                    builder.writeStartObject();
                    builder.writeStringField("key", entry.getKey());
                    builder.writeArrayFieldStart("nodes");
                    for (DiscoveryNode node : entry.getValue()) {
                        builder.writeStartObject();
                        builder.writeStringField("host", node.host());
                        builder.writeNumberField("port", node.port());
                        builder.writeEndObject();
                    }
                    builder.writeEndArray();
                    builder.writeEndObject();
                }
                builder.writeEndArray();
                builder.writeEndObject();
            }
            session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
        } catch (IOException e) {
            session.sendResponse(e);
        }
    }
}
