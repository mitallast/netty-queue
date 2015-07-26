package org.mitallast.queue.rest.action;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StatusRestResponse;
import org.mitallast.queue.rest.transport.HttpResponse;

import java.io.IOException;
import java.io.InputStream;

public class ResourceAction extends BaseRestHandler {

    @Inject
    public ResourceAction(Settings settings, RestController controller) throws IOException {
        super(settings);

        ClassPath classPath = ClassPath.from(getClass().getClassLoader());
        ImmutableSet<ClassPath.ResourceInfo> resources = classPath.getResources();
        resources.stream()
            .filter(resource -> resource.getResourceName().startsWith("META-INF/resources/webjars/"))
            .forEach(resource -> {
                String resourcePath = resource.getResourceName().substring("META-INF".length());
                logger.trace("register {}", resourcePath);
                controller.registerHandler(HttpMethod.GET, resourcePath, this);
                controller.registerHandler(HttpMethod.HEAD, resourcePath, this);
            });

        resources.stream()
            .filter(resource -> resource.getResourceName().startsWith("org/mitallast/queue/admin/"))
            .forEach(resource -> {
                String resourcePath = resource.getResourceName().substring("org/mitallast/queue".length());
                logger.info("register {}", resourcePath);
                controller.registerHandler(HttpMethod.GET, resourcePath, this);
                controller.registerHandler(HttpMethod.HEAD, resourcePath, this);
            });
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        logger.trace("try find {}", request.getQueryPath());
        final InputStream resourceStream;
        if (request.getQueryPath().startsWith("/resources/webjars/")) {
            resourceStream = getClass().getResourceAsStream("/META-INF" + request.getQueryPath());
        } else if (request.getQueryPath().startsWith("/admin/")) {
            resourceStream = getClass().getResourceAsStream("/org/mitallast/queue" + request.getQueryPath());
        } else {
            resourceStream = null;
        }

        if (resourceStream == null) {
            session.sendResponse(new StatusRestResponse(HttpResponseStatus.NOT_FOUND));
        } else {
            try {
                ByteBuf buffer = Unpooled.buffer();
                byte[] bytes = new byte[1024];
                int read;
                do {
                    read = resourceStream.read(bytes);
                    if (read > 0) {
                        buffer.writeBytes(bytes, 0, read);
                    }
                } while (read > 0);
                session.sendResponse(new HttpResponse(HttpResponseStatus.OK, buffer));
            } catch (IOException e) {
                session.sendResponse(e);
            }
        }
    }
}
