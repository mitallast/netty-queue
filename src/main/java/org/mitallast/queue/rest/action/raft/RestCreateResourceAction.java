package org.mitallast.queue.rest.action.raft;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.resource.ResourceService;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StatusRestResponse;

public class RestCreateResourceAction extends BaseRestHandler {

    private final ResourceService resourceService;

    @Inject
    public RestCreateResourceAction(Settings settings, RestController controller, ResourceService resourceService) {
        super(settings);
        this.resourceService = resourceService;
        controller.registerHandler(HttpMethod.PUT, "/_raft/resource", this);
        controller.registerHandler(HttpMethod.POST, "/_raft/resource", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        if (!request.hasParam("path")) {
            session.sendResponse(new StatusRestResponse(HttpResponseStatus.BAD_REQUEST));
            return;
        }
        resourceService.create(request.param("path").toString()).whenComplete((node, error) -> {
            if (error == null) {
                session.sendResponse(new StatusRestResponse(HttpResponseStatus.OK));
            } else {
                session.sendResponse(error);
            }
        });
    }
}
