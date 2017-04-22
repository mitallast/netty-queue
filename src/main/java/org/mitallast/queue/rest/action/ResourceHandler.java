package org.mitallast.queue.rest.action;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.queue.rest.RestController;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

public class ResourceHandler {

    @Inject
    public ResourceHandler(RestController controller) throws IOException {

        ClassPath classPath = ClassPath.from(ResourceHandler.class.getClassLoader());
        ImmutableSet<ClassPath.ResourceInfo> resources = classPath.getResources();

        resources.stream()
            .filter(resource -> resource.getResourceName().startsWith("META-INF/resources/webjars/"))
            .forEach(resource -> {
                String resourcePath = resource.getResourceName().substring("META-INF".length());
                controller.handle(this::webjars)
                    .apply(controller.param().path())
                    .apply(controller.response().url())
                    .handle(HttpMethod.GET, resourcePath);
            });

        File staticFiles = new File("./src/main/resources/org/mitallast/queue/admin/");

        if (!staticFiles.exists()) {
            resources.stream()
                .filter(resource -> resource.getResourceName().startsWith("org/mitallast/queue/admin/"))
                .forEach(resource -> {
                    String resourcePath = resource.getResourceName().substring("org/mitallast/queue/admin/".length());
                    controller.handle(this::resourceStatic)
                        .apply(controller.param().path())
                        .apply(controller.response().url())
                        .handle(HttpMethod.GET, resourcePath);
                });

            controller.handle(this::resourceFavicon)
                .apply(controller.response().url())
                .handle(HttpMethod.GET, "favicon.ico");

            controller.handle(this::resourceIndex)
                .apply(controller.response().url())
                .handle(HttpMethod.GET, "/");
        } else {
            Path root = new File("./src/main/resources/org/mitallast/queue/admin/").toPath();
            Files.walk(root)
                .filter(path -> path.toFile().isFile())
                .forEach(path -> {
                    StringBuilder builder = new StringBuilder();
                    for (Path part : root.getParent().relativize(path)) {
                        if (builder.length() > 0) {
                            builder.append('/');
                        }
                        builder.append(part.getFileName());
                    }
                    String resourcePath = builder.toString();
                    controller.handle(this::fileStatic)
                        .apply(controller.param().path())
                        .apply(controller.response().file())
                        .handle(HttpMethod.GET, resourcePath);
                });

            controller.handle(this::fileFavicon)
                .apply(controller.response().file())
                .handle(HttpMethod.GET, "favicon.ico");

            controller.handle(this::fileIndex)
                .apply(controller.response().file())
                .handle(HttpMethod.GET, "/");
        }
    }

    public URL webjars(String path) {
        return ResourceHandler.class.getResource("/META-INF" + path);
    }

    public URL resourceStatic(String path) {
        return ResourceHandler.class.getResource("/org/mitallast/queue" + path);
    }

    public File fileStatic(String path) {
        return new File("src/main/resources/org/mitallast/queue", path);
    }

    public URL resourceFavicon() {
        return ResourceHandler.class.getResource("/favicon.ico");
    }

    public URL resourceIndex() {
        return ResourceHandler.class.getResource("/org/mitallast/queue/admin/index.html");
    }

    public File fileFavicon() {
        return new File("src/main/resources/favicon.ico");
    }

    public File fileIndex() {
        return new File("src/main/resources/org/mitallast/queue/admin/index.html");
    }
}
