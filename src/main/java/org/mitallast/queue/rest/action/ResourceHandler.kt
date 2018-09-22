package org.mitallast.queue.rest.action

import com.google.common.reflect.ClassPath
import com.google.inject.Inject
import io.netty.handler.codec.http.HttpMethod
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.rest.RestController
import java.io.File
import java.net.URL
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

class ResourceHandler @Inject constructor(c: RestController) {

    init {
        val classPath = ClassPath.from(ResourceHandler::class.java.classLoader)
        val resources = classPath.resources

        resources
            .stream()
            .filter { resource -> resource.resourceName.startsWith("META-INF/resources/webjars/") }
            .forEach { resource ->
                val resourcePath = resource.resourceName.substring("META-INF".length)
                c.handle(this::webjars, c.param().path(), c.response().url())
                    .handle(HttpMethod.GET, resourcePath)
            }

        val staticFiles = File("./src/main/resources/org/mitallast/queue/admin/")

        if (!staticFiles.exists()) {
            resources.stream()
                .filter { resource -> resource.resourceName.startsWith("org/mitallast/queue/admin/") }
                .forEach { resource ->
                    val resourcePath = resource.resourceName.substring("org/mitallast/queue".length)
                    c.handle(this::resourceStatic, c.param().path(), c.response().url())
                        .handle(HttpMethod.GET, resourcePath)
                }

            c.handle(this::resourceFavicon, c.response().url())
                .handle(HttpMethod.GET, "favicon.ico")

            c.handle(this::resourceIndex, c.response().url())
                .handle(HttpMethod.GET, "/")
        } else {
            val root = File("./src/main/resources/org/mitallast/queue/admin/").toPath()
            Files.walk(root)
                .filter { path -> path.toFile().isFile }
                .forEach { path ->
                    val builder = StringBuilder()
                    for (part in root.parent.relativize(path)) {
                        if (builder.isNotEmpty()) {
                            builder.append('/')
                        }
                        builder.append(part.fileName)
                    }
                    val resourcePath = builder.toString()
                    c.handle(this::fileStatic, c.param().path(), c.response().file())
                        .handle(HttpMethod.GET, resourcePath)
                }

            c.handle(this::fileFavicon, c.response().file())
                .handle(HttpMethod.GET, "favicon.ico")

            c.handle(this::fileIndex, c.response().file())
                .handle(HttpMethod.GET, "/")
        }
        c.handle(this::index, c.response().text())
            .handle(HttpMethod.GET, "/_index")
    }

    private val logger = LogManager.getLogger()
    private val counter = AtomicLong(0)

    private fun index(): String {
        return "OK"
    }

    private fun webjars(path: String): URL {
        return ResourceHandler::class.java.getResource("/META-INF" + path)
    }

    private fun resourceStatic(path: String): URL {
        return ResourceHandler::class.java.getResource("/org/mitallast/queue" + path)
    }

    private fun fileStatic(path: String): File {
        return File("src/main/resources/org/mitallast/queue", path)
    }

    private fun resourceFavicon(): URL {
        return ResourceHandler::class.java.getResource("/favicon.ico")
    }

    private fun resourceIndex(): URL {
        return ResourceHandler::class.java.getResource("/org/mitallast/queue/admin/index.html")
    }

    private fun fileFavicon(): File {
        return File("src/main/resources/favicon.ico")
    }

    private fun fileIndex(): File {
        return File("src/main/resources/org/mitallast/queue/admin/index.html")
    }
}
