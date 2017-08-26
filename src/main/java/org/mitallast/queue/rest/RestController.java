package org.mitallast.queue.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import javaslang.*;
import javaslang.concurrent.Future;
import javaslang.control.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.json.JsonService;
import org.mitallast.queue.common.path.PathTrie;
import org.mitallast.queue.rest.netty.HttpRequest;

import java.io.File;
import java.net.URL;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class RestController {
    private final static Logger logger = LogManager.getLogger();

    private final JsonService jsonService;

    private final PathTrie<Consumer<RestRequest>> getHandlers = new PathTrie<>();
    private final PathTrie<Consumer<RestRequest>> postHandlers = new PathTrie<>();
    private final PathTrie<Consumer<RestRequest>> putHandlers = new PathTrie<>();
    private final PathTrie<Consumer<RestRequest>> deleteHandlers = new PathTrie<>();
    private final PathTrie<Consumer<RestRequest>> headHandlers = new PathTrie<>();
    private final PathTrie<Consumer<RestRequest>> optionsHandlers = new PathTrie<>();

    private final ResponseMappers responseMappers;
    private final ParamMappers paramMappers;

    @Inject
    public RestController(JsonService jsonService) {
        this.jsonService = jsonService;

        this.responseMappers = new ResponseMappers();
        this.paramMappers = new ParamMappers();
    }

    public void dispatchRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        final HttpRequest request = new HttpRequest(ctx, httpRequest, jsonService);
        try {
            executeHandler(request);
        } catch (Throwable e) {
            logger.warn("error process request {} {}", request.getHttpMethod(), request.getUri());
            logger.warn("unexpected exception", e);
            try {
                request.response()
                    .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    .error(e);
            } catch (Throwable ex) {
                logger.error("error send", e);
                logger.error("Failed to send failure response for uri [" + httpRequest.uri() + "]", ex);
            }
        } finally {
            httpRequest.release();
        }
    }

    private void executeHandler(RestRequest request) {
        final Consumer<RestRequest> handler = getHandler(request);
        if (handler != null) {
            handler.accept(request);
        } else {
            logger.warn("handler not found for {} {}", request.getHttpMethod(), request.getUri());
            if (request.getHttpMethod() == HttpMethod.OPTIONS) {
                request.response()
                    .status(HttpResponseStatus.OK)
                    .empty();
            } else {
                request.response()
                    .status(HttpResponseStatus.BAD_REQUEST)
                    .text("No handler found for uri [" + request.getUri() + "] and method [" + request.getHttpMethod() + "]");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Consumer<RestRequest> getHandler(RestRequest request) {
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

    public void register(HttpMethod method, String path, Consumer<RestRequest> handler) {
        logger.info("register: {} {}", method, path);
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

    /*
     * Functional API
     */

    public ResponseMappers response() {
        return responseMappers;
    }

    public ParamMappers param() {
        return paramMappers;
    }

    public <R> Function1<
        BiConsumer<RestRequest, R>,
        FunctionHandlerBuilder> handle(Function0<R> handler) {
        return mr -> new FunctionHandlerBuilder(request -> {
            R r = handler.apply();
            mr.accept(request, r);
        });
    }

    public <P1, R> Function2<
        Function1<RestRequest, P1>,
        BiConsumer<RestRequest, R>,
        FunctionHandlerBuilder> handle(Function1<P1, R> handler) {
        return (m1, mr) -> new FunctionHandlerBuilder(request -> {
            P1 p1 = m1.apply(request);
            R r = handler.apply(p1);
            mr.accept(request, r);
        });
    }

    public <P1, P2, R> Function3<
        Function1<RestRequest, P1>,
        Function1<RestRequest, P2>,
        BiConsumer<RestRequest, R>,
        FunctionHandlerBuilder> handle(Function2<P1, P2, R> handler) {
        return (m1, m2, mr) -> new FunctionHandlerBuilder(request -> {
            P1 p1 = m1.apply(request);
            P2 p2 = m2.apply(request);
            R r = handler.apply(p1, p2);
            mr.accept(request, r);
        });
    }

    public <P1, P2, P3, R> Function4<
        Function1<RestRequest, P1>,
        Function1<RestRequest, P2>,
        Function1<RestRequest, P3>,
        BiConsumer<RestRequest, R>,
        FunctionHandlerBuilder> handle(Function3<P1, P2, P3, R> handler) {
        return (m1, m2, m3, mr) -> new FunctionHandlerBuilder(request -> {
            P1 p1 = m1.apply(request);
            P2 p2 = m2.apply(request);
            P3 p3 = m3.apply(request);
            R r = handler.apply(p1, p2, p3);
            mr.accept(request, r);
        });
    }

    // Functional mappers

    public static final class ResponseMappers {

        public Consumer<RestRequest> notFound() {
            return request -> request.response().status(HttpResponseStatus.NOT_FOUND).empty();
        }

        public Consumer<RestRequest> ok() {
            return request -> request.response().status(HttpResponseStatus.OK).empty();
        }

        public Consumer<RestRequest> created() {
            return request -> request.response().status(HttpResponseStatus.CREATED).empty();
        }

        public Consumer<RestRequest> badRequest() {
            return request -> request.response().status(HttpResponseStatus.BAD_REQUEST).empty();
        }

        public BiConsumer<RestRequest, Boolean> either(Consumer<RestRequest> right, Consumer<RestRequest> left) {
            return (request, value) -> {
                if (value) {
                    right.accept(request);
                } else {
                    left.accept(request);
                }
            };
        }

        public <T> BiConsumer<RestRequest, T> text() {
            return (request, response) -> request.response().text(response.toString());
        }

        public BiConsumer<RestRequest, byte[]> bytes() {
            return (request, response) -> request.response().bytes(response);
        }

        public <T> BiConsumer<RestRequest, T> json() {
            return (request, t) -> request.response().json(t);
        }

        public <T> BiConsumer<RestRequest, Option<T>> optionalJson() {
            return optional(json());
        }

        public BiConsumer<RestRequest, URL> url() {
            return (request, file) -> request.response().file(file);
        }

        public BiConsumer<RestRequest, Option<URL>> optionalUrl() {
            return optional(url());
        }

        public BiConsumer<RestRequest, File> file() {
            return (request, file) -> request.response().file(file);
        }

        public <T> BiConsumer<RestRequest, Option<T>> optional(BiConsumer<RestRequest, T> mapper) {
            return optional(mapper, notFound());
        }

        public <T> BiConsumer<RestRequest, Option<T>> optional(BiConsumer<RestRequest, T> mapper,
                                                               Consumer<RestRequest> empty) {
            return (request, optional) -> {
                if (optional.isDefined()) {
                    mapper.accept(request, optional.get());
                } else {
                    empty.accept(request);
                }
            };
        }

        public <T> BiConsumer<RestRequest, Future<T>> futureJson() {
            return future(json());
        }

        public BiConsumer<RestRequest, Future<Boolean>> futureEither(Consumer<RestRequest> right, Consumer<RestRequest> left) {
            return future(either(right, left));
        }

        public <T> BiConsumer<RestRequest, Future<T>> future(BiConsumer<RestRequest, T> mapper) {
            return (request, future) -> future.onComplete(result -> {
                if (result.isSuccess()) {
                    mapper.accept(request, result.get());
                } else {
                    request.response()
                        .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                        .error(result.getCause());
                }
            });
        }
    }

    public final class ParamMappers {
        public Function1<RestRequest, RestRequest> request() {
            return request -> request;
        }

        public Function1<RestRequest, ByteBuf> content() {
            return RestRequest::content;
        }

        public Function1<RestRequest, HttpMethod> method() {
            return RestRequest::getHttpMethod;
        }

        public Function1<RestRequest, String> uri() {
            return RestRequest::getUri;
        }

        public Function1<RestRequest, String> path() {
            return RestRequest::getQueryPath;
        }

        public Function1<RestRequest, String> string(String name) {
            return request -> request.param(name);
        }

        public Function1<RestRequest, Integer> toInt(String name) {
            return string(name).andThen(Integer::valueOf);
        }

        public Function1<RestRequest, Long> toLong(String name) {
            return string(name).andThen(Long::valueOf);
        }

        public Function1<RestRequest, Boolean> toBoolean(String name) {
            return string(name).andThen(Boolean::valueOf);
        }

        public <T> Function1<RestRequest, T> json(Class<T> type) {
            return request -> jsonService.deserialize(request.content(), type);
        }

        public <T> Function1<RestRequest, T> json(TypeReference<T> type) {
            return request -> jsonService.deserialize(request.content(), type);
        }
    }

    public final class FunctionHandlerBuilder {
        private final Consumer<RestRequest> handler;

        public FunctionHandlerBuilder(Consumer<RestRequest> handler) {
            this.handler = handler;
        }

        public void handle(HttpMethod method, String path) {
            register(method, path, handler);
        }

        public void handle(HttpMethod method1, HttpMethod method2, String path) {
            register(method1, path, handler);
            register(method2, path, handler);
        }
    }
}
