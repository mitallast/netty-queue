package org.mitallast.queue.rest

import com.fasterxml.jackson.core.type.TypeReference
import com.google.inject.Inject
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.FullHttpRequest
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpResponseStatus
import javaslang.concurrent.Future
import javaslang.control.Option
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.common.json.JsonService
import org.mitallast.queue.common.path.PathTrie
import org.mitallast.queue.rest.netty.HttpRequest
import java.io.File
import java.net.URL

class RestController @Inject constructor(private val jsonService: JsonService) {
    private val logger = LogManager.getLogger()
    private val getHandlers = PathTrie<(RestRequest) -> Unit>()
    private val postHandlers = PathTrie<(RestRequest) -> Unit>()
    private val putHandlers = PathTrie<(RestRequest) -> Unit>()
    private val deleteHandlers = PathTrie<(RestRequest) -> Unit>()
    private val headHandlers = PathTrie<(RestRequest) -> Unit>()
    private val optionsHandlers = PathTrie<(RestRequest) -> Unit>()

    private val responseMappers: ResponseMappers
    private val paramMappers: ParamMappers

    init {

        this.responseMappers = ResponseMappers()
        this.paramMappers = ParamMappers()
    }

    fun dispatchRequest(ctx: ChannelHandlerContext, httpRequest: FullHttpRequest) {
        val request = HttpRequest(ctx, httpRequest, jsonService)
        try {
            executeHandler(request)
        } catch (e: Throwable) {
            logger.warn("error process request {} {}", request.httpMethod, request.uri)
            logger.warn("unexpected exception", e)
            try {
                request.response()
                    .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    .error(e)
            } catch (ex: Throwable) {
                logger.error("error send", e)
                logger.error("Failed to send failure response for uri [" + httpRequest.uri() + "]", ex)
            }

        } finally {
            httpRequest.release()
        }
    }

    private fun executeHandler(request: RestRequest) {
        val handler = getHandler(request)
        if (handler != null) {
            handler.invoke(request)
        } else {
            logger.warn("handler not found for {} {}", request.httpMethod, request.uri)
            if (request.httpMethod === HttpMethod.OPTIONS) {
                request.response()
                    .status(HttpResponseStatus.OK)
                    .empty()
            } else {
                request.response()
                    .status(HttpResponseStatus.BAD_REQUEST)
                    .text("No handler found for uri [" + request.uri + "] and method [" + request.httpMethod + "]")
            }
        }
    }

    private fun getHandler(request: RestRequest): ((RestRequest) -> Unit)? {
        val path = request.queryPath
        val method = request.httpMethod
        return when {
            method === HttpMethod.GET -> getHandlers.retrieve(path, request.paramMap)
            method === HttpMethod.POST -> postHandlers.retrieve(path, request.paramMap)
            method === HttpMethod.PUT -> putHandlers.retrieve(path, request.paramMap)
            method === HttpMethod.DELETE -> deleteHandlers.retrieve(path, request.paramMap)
            method === HttpMethod.HEAD -> headHandlers.retrieve(path, request.paramMap)
            method === HttpMethod.OPTIONS -> optionsHandlers.retrieve(path, request.paramMap)
            else -> null
        }
    }

    fun register(method: HttpMethod, path: String, handler: (RestRequest) -> Unit) {
        logger.info("register: {} {}", method, path)
        when {
            HttpMethod.GET === method -> getHandlers.insert(path, handler)
            HttpMethod.DELETE === method -> deleteHandlers.insert(path, handler)
            HttpMethod.POST === method -> postHandlers.insert(path, handler)
            HttpMethod.PUT === method -> putHandlers.insert(path, handler)
            HttpMethod.OPTIONS === method -> optionsHandlers.insert(path, handler)
            HttpMethod.HEAD === method -> headHandlers.insert(path, handler)
            else -> throw IllegalArgumentException("Can't handle [$method] for path [$path]")
        }
    }

    /*
     * Functional API
     */

    fun response(): ResponseMappers {
        return responseMappers
    }

    fun param(): ParamMappers {
        return paramMappers
    }

    fun <R> handle(
        handler: () -> R,
        mr: (RestRequest, R) -> Unit
    ): FunctionHandlerBuilder {
        return FunctionHandlerBuilder({ request ->
            val r = handler.invoke()
            mr.invoke(request, r)
        })
    }

    fun <P1, R> handle(
        handler: (P1) -> R,
        m1: (RestRequest) -> P1,
        mr: (RestRequest, R) -> Unit
    ): FunctionHandlerBuilder {
        return FunctionHandlerBuilder({ request ->
            val p1 = m1.invoke(request)
            val r = handler.invoke(p1)
            mr.invoke(request, r)
        })
    }

    fun <P1, P2, R> handle(
        handler: (P1, P2) -> R,
        m1: (RestRequest) -> P1,
        m2: (RestRequest) -> P2,
        mr: (RestRequest, R) -> Unit
    ): FunctionHandlerBuilder {
        return FunctionHandlerBuilder({ request ->
            val p1 = m1.invoke(request)
            val p2 = m2.invoke(request)
            val r = handler.invoke(p1, p2)
            mr.invoke(request, r)
        })
    }

    fun <P1, P2, P3, R> handle(
        handler: (P1, P2, P3) -> R,
        m1: (RestRequest) -> P1,
        m2: (RestRequest) -> P2,
        m3: (RestRequest) -> P3,
        mr: (RestRequest, R) -> Unit
    ): FunctionHandlerBuilder {
        return FunctionHandlerBuilder({ request ->
            val p1 = m1.invoke(request)
            val p2 = m2.invoke(request)
            val p3 = m3.invoke(request)
            val r = handler.invoke(p1, p2, p3)
            mr.invoke(request, r)
        })
    }

    // Functional mappers

    class ResponseMappers {

        fun notFound(): (RestRequest) -> Unit {
            return { request -> request.response().status(HttpResponseStatus.NOT_FOUND).empty() }
        }

        fun ok(): (RestRequest) -> Unit {
            return { request -> request.response().status(HttpResponseStatus.OK).empty() }
        }

        fun created(): (RestRequest) -> Unit {
            return { request -> request.response().status(HttpResponseStatus.CREATED).empty() }
        }

        fun badRequest(): (RestRequest) -> Unit {
            return { request -> request.response().status(HttpResponseStatus.BAD_REQUEST).empty() }
        }

        fun either(right: (RestRequest) -> Unit, left: (RestRequest) -> Unit): (RestRequest, Boolean) -> Unit {
            return { request, value ->
                if (value) {
                    right.invoke(request)
                } else {
                    left.invoke(request)
                }
            }
        }

        fun <T> text(): (RestRequest, T) -> Unit {
            return { request, response -> request.response().text(response.toString()) }
        }

        fun bytes(): (RestRequest, ByteArray) -> Unit {
            return { request, response -> request.response().bytes(response) }
        }

        fun <T: Any> json(): (RestRequest, T) -> Unit {
            return { request, t -> request.response().json(t) }
        }

        fun <T: Any> optionalJson(): (RestRequest, Option<T>) -> Unit {
            return optional(json())
        }

        fun url(): (RestRequest, URL) -> Unit {
            return { request, file -> request.response().file(file) }
        }

        fun optionalUrl(): (RestRequest, Option<URL>) -> Unit {
            return optional(url())
        }

        fun file(): (RestRequest, File) -> Unit {
            return { request, file -> request.response().file(file) }
        }

        fun <T> optional(mapper: (RestRequest, T) -> Unit): (RestRequest, Option<T>) -> Unit {
            return optional(mapper, notFound())
        }

        fun <T> optional(mapper: (RestRequest, T) -> Unit,
                         empty: (RestRequest) -> Unit): (RestRequest, Option<T>) -> Unit {
            return { request, optional ->
                if (optional.isDefined) {
                    mapper.invoke(request, optional.get())
                } else {
                    empty.invoke(request)
                }
            }
        }

        fun <T: Any> futureJson(): (RestRequest, Future<T>) -> Unit {
            return future(json())
        }

        fun futureEither(right: (RestRequest) -> Unit,
                         left: (RestRequest) -> Unit): (RestRequest, Future<Boolean>) -> Unit {
            return future(either(right, left))
        }

        fun <T> future(mapper: (RestRequest, T) -> Unit): (RestRequest, Future<T>) -> Unit {
            return { request, future ->
                future.onComplete({ result ->
                    if (result.isSuccess) {
                        mapper.invoke(request, result.get())
                    } else {
                        request.response()
                            .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                            .error(result.cause)
                    }
                })
            }
        }
    }

    inner class ParamMappers {
        fun request(): (RestRequest) -> RestRequest {
            return { it }
        }

        fun content(): (RestRequest) -> ByteBuf {
            return { it.content }
        }

        fun method(): (RestRequest) -> HttpMethod {
            return { it.httpMethod }
        }

        fun uri(): (RestRequest) -> String {
            return { it.uri }
        }

        fun path(): (RestRequest) -> String {
            return { it.queryPath }
        }

        fun string(name: String): (RestRequest) -> String {
            return { request -> request.param(name) }
        }

        fun toInt(name: String): (RestRequest) -> Int {
            return { request -> request.param(name).toInt() }
        }

        fun toLong(name: String): (RestRequest) -> Long {
            return { request -> request.param(name).toLong() }
        }

        fun toBoolean(name: String): (RestRequest) -> Boolean {
            return { request -> request.param(name).toBoolean() }
        }

        fun <T> json(type: Class<T>): (RestRequest) -> T {
            return { request -> jsonService.deserialize<T>(request.content, type) }
        }

        fun <T> json(type: TypeReference<T>): (RestRequest) -> T {
            return { request -> jsonService.deserialize<T>(request.content, type) }
        }
    }

    inner class FunctionHandlerBuilder(private val handler: (RestRequest) -> Unit) {

        fun handle(method: HttpMethod, path: String) {
            register(method, path, handler)
        }

        fun handle(method1: HttpMethod, method2: HttpMethod, path: String) {
            register(method1, path, handler)
            register(method2, path, handler)
        }
    }
}
