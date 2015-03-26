package org.mitallast.queue.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.path.PathTrie;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.response.StatusRestResponse;
import org.mitallast.queue.rest.response.StringRestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestController extends AbstractComponent {

    private final Logger logger = LoggerFactory.getLogger(RestController.class);

    private final PathTrie<RestHandler> getHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> postHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> putHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> deleteHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> headHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> optionsHandlers = new PathTrie<>();

    @Inject
    public RestController(Settings settings) {
        super(settings);
    }

    public void dispatchRequest(RestRequest request, RestSession channel) {
        try {
            executeHandler(request, channel);
        } catch (Throwable e) {
            try {
                channel.sendResponse(e);
            } catch (Throwable ex) {
                logger.error("error send", e);
                logger.error("Failed to send failure response for uri [" + request.getUri() + "]", ex);
            }
        }
    }

    private void executeHandler(RestRequest request, RestSession channel) {
        final RestHandler handler = getHandler(request);
        if (handler != null) {
            handler.handleRequest(request, channel);
        } else {
            if (request.getHttpMethod() == HttpMethod.OPTIONS) {
                // when we have OPTIONS request, simply send OK by default (with the Access Control Origin header which gets automatically added)
                channel.sendResponse(new StatusRestResponse(HttpResponseStatus.OK));
            } else {
                channel.sendResponse(new StringRestResponse(HttpResponseStatus.BAD_REQUEST, "No handler found for uri [" + request.getUri() + "] and method [" + request.getHttpMethod() + "]"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private RestHandler getHandler(RestRequest request) {
        String path = request.getQueryPath();
        HttpMethod method = request.getHttpMethod();
        if (method == HttpMethod.GET) {
            return getHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.POST) {
            return postHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.PUT) {
            return putHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.DELETE) {
            return deleteHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.HEAD) {
            return headHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.OPTIONS) {
            return optionsHandlers.retrieve(path, request.getParamMap());
        } else {
            return null;
        }
    }

    public void registerHandler(HttpMethod method, String path, RestHandler handler) {
        if (HttpMethod.GET == method) {
            getHandlers.insert(path, handler);
        } else if (HttpMethod.DELETE == method) {
            deleteHandlers.insert(path, handler);
        } else if (HttpMethod.POST == method) {
            postHandlers.insert(path, handler);
        } else if (HttpMethod.PUT == method) {
            putHandlers.insert(path, handler);
        } else if (HttpMethod.OPTIONS == method) {
            optionsHandlers.insert(path, handler);
        } else if (HttpMethod.HEAD == method) {
            headHandlers.insert(path, handler);
        } else {
            throw new IllegalArgumentException("Can't handle [" + method + "] for path [" + path + "]");
        }
    }
}
