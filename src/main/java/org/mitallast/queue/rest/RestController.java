package org.mitallast.queue.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import javaslang.control.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.json.JsonService;
import org.mitallast.queue.common.path.PathTrie;
import org.mitallast.queue.rest.netty.HttpRequest;

import java.io.File;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class RestController {
    private final static Logger logger = LogManager.getLogger();

    private final JsonService jsonService;

    private final PathTrie<RestHandler> getHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> postHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> putHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> deleteHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> headHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> optionsHandlers = new PathTrie<>();

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

    private void executeHandler(RestRequest request)  {
        final RestHandler handler = getHandler(request);
        if (handler != null) {
            handler.handleRequest(request);
        } else {
            logger.warn("handler not found for {} {}", request.getHttpMethod(), request.getUri());
            if (request.getHttpMethod() == HttpMethod.OPTIONS) {
                request.response()
                    .status(HttpResponseStatus.OK)
                    .empty();
            } else {
                request.response()
                    .status(HttpResponseStatus.BAD_REQUEST)
                    .text("No handler found for uri [" + request.getUri() + "] and method [" + request.getHttpMethod
                        () + "]");
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

    public void register(HttpMethod method, String path, RestHandler handler) {
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

    public <R> FunctionResponseMapper<R> handler(FunctionHandler<R> handler) {
        return new FunctionResponseMapper<>(handler);
    }

    public <R, P1> FunctionParameter1Mapper<R, P1> handler(Function1Handler<R, P1> handler) {
        return new FunctionParameter1Mapper<>(handler);
    }

    public <R, P1, P2> FunctionParameter2Mapper<R, P1, P2> handler(Function2Handler<R, P1, P2> handler) {
        return new FunctionParameter2Mapper<>(handler);
    }

    public <R, P1, P2, P3> FunctionParameter3Mapper<R, P1, P2, P3> handler(Function3Handler<R, P1, P2, P3> handler) {
        return new FunctionParameter3Mapper<>(handler);
    }

    // Functional mappers

    public final class ResponseMappers {

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

        public BiConsumer<RestRequest, String> text() {
            return (request, response) -> request.response().text(response);
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

        public <T> BiConsumer<RestRequest, CompletableFuture<T>> futureJson() {
            return future(json());
        }

        public BiConsumer<RestRequest, CompletableFuture<byte[]>> futureBytes() {
            return future(bytes());
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

        public <T> BiConsumer<RestRequest, CompletableFuture<T>> future(BiConsumer<RestRequest, T> mapper) {
            return (request, future) -> future.whenComplete((value, error) -> {
                if (error != null) {
                    request.response().status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                        .error(error);
                } else {
                    mapper.accept(request, value);
                }
            });
        }
    }

    public final class ParamMappers {
        public Function<RestRequest, RestRequest> request() {
            return request -> request;
        }

        public Function<RestRequest, ByteBuf> content() {
            return RestRequest::content;
        }

        public Function<RestRequest, HttpMethod> method() {
            return RestRequest::getHttpMethod;
        }

        public Function<RestRequest, String> uri() {
            return RestRequest::getUri;
        }

        public Function<RestRequest, String> path() {
            return RestRequest::getQueryPath;
        }

        public Function<RestRequest, String> string(String name) {
            return request -> request.param(name);
        }

        public Function<RestRequest, Integer> toInt(String name) {
            return string(name).andThen(Integer::valueOf);
        }

        public Function<RestRequest, Long> toLong(String name) {
            return string(name).andThen(Long::valueOf);
        }

        public Function<RestRequest, Boolean> toBoolean(String name) {
            return string(name).andThen(Boolean::valueOf);
        }

        public <T> Function<RestRequest, T> json(Class<T> type) {
            return request -> jsonService.deserialize(request.content(), type);
        }

        public <T> Function<RestRequest, T> json(TypeReference<T> type) {
            return request -> jsonService.deserialize(request.content(), type);
        }
    }

    // function handler parameter mapper builder

    public final class FunctionParameter3Mapper<R, P1, P2, P3> {
        private final Function3Builder<R, P1, P2, P3> builder;

        public FunctionParameter3Mapper(Function3Handler<R, P1, P2, P3> handler) {
            this.builder = param1Mapper -> param2Mapper -> param3Mapper -> responseMapper -> new Function3Mapper<>
                (handler, param1Mapper, param2Mapper, param3Mapper, responseMapper);
        }

        public FunctionParameter3Mapper(Function3Builder<R, P1, P2, P3> builder) {
            this.builder = builder;
        }

        public FunctionParameter2Mapper<R, P2, P3> param(Function<RestRequest, P1> paramMapper) {
            return new FunctionParameter2Mapper<>(builder.build(paramMapper));
        }
    }

    public final class FunctionParameter2Mapper<R, P1, P2> {

        private final Function2Builder<R, P1, P2> builder;

        public FunctionParameter2Mapper(Function2Handler<R, P1, P2> handler) {
            builder = param1Mapper -> param2Mapper -> responseMapper -> new Function2Mapper<>(handler, param1Mapper,
                param2Mapper, responseMapper);
        }

        public FunctionParameter2Mapper(Function2Builder<R, P1, P2> builder) {
            this.builder = builder;
        }

        public FunctionParameter1Mapper<R, P2> param(Function<RestRequest, P1> paramMapper) {
            return new FunctionParameter1Mapper<>(builder.build(paramMapper));
        }
    }

    public final class FunctionParameter1Mapper<R, P1> {
        private final Function1Builder<R, P1> builder;

        public FunctionParameter1Mapper(Function1Handler<R, P1> handler) {
            builder = paramMapper -> responseMapper -> new Function1Mapper<>(handler, paramMapper, responseMapper);
        }

        public FunctionParameter1Mapper(Function1Builder<R, P1> builder) {
            this.builder = builder;
        }

        public FunctionResponseMapper<R> param(Function<RestRequest, P1> paramMapper) {
            return new FunctionResponseMapper<>(builder.build(paramMapper));
        }
    }

    // function handler response mapper builder

    public class FunctionResponseMapper<R> {
        private final FunctionBuilder<R> builder;

        public FunctionResponseMapper(FunctionHandler<R> handler) {
            this.builder = responseMapper -> new FunctionMapper<>(handler, responseMapper);
        }

        public FunctionResponseMapper(FunctionBuilder<R> builder) {
            this.builder = builder;
        }

        public FunctionHandlerBuilder response(BiConsumer<RestRequest, R> responseMapper) {
            return new FunctionHandlerBuilder(builder.build(responseMapper));
        }
    }

    // function rest handler builder

    public final class FunctionHandlerBuilder {
        private final RestHandler handler;

        public FunctionHandlerBuilder(RestHandler handler) {
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

    // function handler mapper

    public final class FunctionMapper<R> implements RestHandler {
        private final FunctionHandler<R> handler;
        private final BiConsumer<RestRequest, R> responseMapper;

        public FunctionMapper(FunctionHandler<R> handler, BiConsumer<RestRequest, R> responseMapper) {
            this.handler = handler;
            this.responseMapper = responseMapper;
        }

        @Override
        public void handleRequest(RestRequest request)  {
            R response = handler.handleRequest();
            responseMapper.accept(request, response);
        }
    }

    public final class Function1Mapper<R, P1> implements RestHandler {
        private final Function1Handler<R, P1> handler;
        private final Function<RestRequest, P1> param1mapper;
        private final BiConsumer<RestRequest, R> responseMapper;

        public Function1Mapper(
            Function1Handler<R, P1> handler,
            Function<RestRequest, P1> param1mapper,
            BiConsumer<RestRequest, R> responseMapper
        ) {
            this.handler = handler;
            this.param1mapper = param1mapper;
            this.responseMapper = responseMapper;
        }

        @Override
        public void handleRequest(RestRequest request)  {
            P1 param1 = param1mapper.apply(request);
            R response = handler.handleRequest(param1);
            responseMapper.accept(request, response);
        }
    }

    public final class Function2Mapper<R, P1, P2> implements RestHandler {
        private final Function2Handler<R, P1, P2> handler;
        private final Function<RestRequest, P1> param1mapper;
        private final Function<RestRequest, P2> param2mapper;
        private final BiConsumer<RestRequest, R> responseMapper;

        public Function2Mapper(
            Function2Handler<R, P1, P2> handler,
            Function<RestRequest, P1> param1mapper,
            Function<RestRequest, P2> param2mapper,
            BiConsumer<RestRequest, R> responseMapper
        ) {
            this.handler = handler;
            this.param1mapper = param1mapper;
            this.param2mapper = param2mapper;
            this.responseMapper = responseMapper;
        }

        @Override
        public void handleRequest(RestRequest request)  {
            P1 param1 = param1mapper.apply(request);
            P2 param2 = param2mapper.apply(request);
            R response = handler.handleRequest(param1, param2);
            responseMapper.accept(request, response);
        }
    }

    public final class Function3Mapper<R, P1, P2, P3> implements RestHandler {
        private final Function3Handler<R, P1, P2, P3> handler;
        private final Function<RestRequest, P1> param1mapper;
        private final Function<RestRequest, P2> param2mapper;
        private final Function<RestRequest, P3> param3mapper;
        private final BiConsumer<RestRequest, R> responseMapper;

        public Function3Mapper(
            Function3Handler<R, P1, P2, P3> handler,
            Function<RestRequest, P1> param1mapper,
            Function<RestRequest, P2> param2mapper,
            Function<RestRequest, P3> param3mapper,
            BiConsumer<RestRequest, R> responseMapper
        ) {
            this.handler = handler;
            this.param1mapper = param1mapper;
            this.param2mapper = param2mapper;
            this.param3mapper = param3mapper;
            this.responseMapper = responseMapper;
        }

        @Override
        public void handleRequest(RestRequest request)  {
            P1 param1 = param1mapper.apply(request);
            P2 param2 = param2mapper.apply(request);
            P3 param3 = param3mapper.apply(request);
            R response = handler.handleRequest(param1, param2, param3);
            responseMapper.accept(request, response);
        }
    }

    // function handler curring

    public interface Function3Builder<R, P1, P2, P3> {
        Function2Builder<R, P2, P3> build(Function<RestRequest, P1> mapper);
    }

    public interface Function2Builder<R, P1, P2> {
        Function1Builder<R, P2> build(Function<RestRequest, P1> mapper);
    }

    public interface Function1Builder<R, P1> {
        FunctionBuilder<R> build(Function<RestRequest, P1> mapper);
    }

    public interface FunctionBuilder<R> {
        RestHandler build(BiConsumer<RestRequest, R> mapper);
    }

    // function handler

    public interface FunctionHandler<R> {
        R handleRequest() ;
    }

    public interface Function1Handler<R, P1> {
        R handleRequest(P1 p1) ;
    }

    public interface Function2Handler<R, P1, P2> {
        R handleRequest(P1 p1, P2 p2) ;
    }

    public interface Function3Handler<R, P1, P2, P3> {
        R handleRequest(P1 p1, P2 p2, P3 p3) ;
    }
}
