package org.mitallast.queue.rest.action.blob;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.blob.DistributedStorageService;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

public class GetBlobResourceAction extends BaseRestHandler {

    private final DistributedStorageService storageService;

    @Inject
    public GetBlobResourceAction(Config config, RestController controller, DistributedStorageService storageService) {
        super(config.getConfig("rest"), PutBlobResourceAction.class);
        this.storageService = storageService;

        controller.registerHandler(HttpMethod.GET, "/_blob/{key}", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        request.content().release();
        CharSequence key = request.param("key");
        storageService.getResource(key.toString()).whenComplete((getBlobResourceResponse, error) -> {
            if (error != null) {
                session.sendResponse(error);
            } else {
                ByteBuf buf = Unpooled.wrappedBuffer(getBlobResourceResponse.getData());
                session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buf));
            }
        });
    }
}
