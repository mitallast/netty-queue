package org.mitallast.queue.rest.action.blob;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.blob.DistributedStorageService;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

import java.io.IOException;

public class PutBlobResourceAction extends BaseRestHandler {

    private final DistributedStorageService storageService;

    @Inject
    public PutBlobResourceAction(Config config, RestController controller, DistributedStorageService storageService) {
        super(config.getConfig("rest"), PutBlobResourceAction.class);
        this.storageService = storageService;

        controller.registerHandler(HttpMethod.PUT, "/_blob/{key}", this);
        controller.registerHandler(HttpMethod.POST, "/_blob/{key}", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        CharSequence key = request.param("key");
        int size = request.content().readableBytes();
        byte[] data = new byte[size];
        request.content().readBytes(data);
        request.content().release();

        storageService.putResource(key.toString(), data).whenComplete((stored, error) -> {
            if (error != null) {
                session.sendResponse(error);
            } else {
                ByteBuf buffer = session.alloc().directBuffer();
                try {
                    try (XStreamBuilder builder = createBuilder(request, buffer)) {
                        builder.writeStartObject();
                        builder.writeBooleanField("stored", stored);
                        builder.writeEndObject();
                    }
                    session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
                } catch (IOException e) {
                    session.sendResponse(e);
                }
            }
        });
    }
}
