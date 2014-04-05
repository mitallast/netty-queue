package org.mitallast.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.mitallast.transport.http.route.Route;
import org.mitallast.transport.http.route.RouteResolver;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Request {

    public static final String ENCODING = "UTF-8";
    public static final String METHOD_TUNNEL = "_method";
    private static final String DEFAULT_PROTOCOL = "http";
    private FullHttpRequest httpRequest;
    private HttpMethod httpMethod;
    private HttpHeaders httpHeaders;

    private RouteResolver routeResolver;
    private Route resolvedRoute;

    private ObjectMapper mapper;

    private Map<String, String> queryStringMap;

    public Request(FullHttpRequest request, RouteResolver routeResolver, ObjectMapper mapper) {
        this.httpRequest = request;
        this.httpMethod = request.getMethod();
        this.httpHeaders = request.headers();
        this.routeResolver = routeResolver;
        this.mapper = mapper;
        this.parseQueryString();
        determineEffectiveHttpMethod();
    }

    public FullHttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

    public RouteResolver getRouteResolver() {
        return routeResolver;
    }

    public Route getResolvedRoute() {
        return resolvedRoute;
    }

    public Map<String, String> getQueryStringMap() {
        return queryStringMap;
    }

    public HttpHeaders getHttpHeaders() {
        return httpHeaders;
    }

    public boolean isMethodGet() {
        return httpMethod.equals(HttpMethod.GET);
    }

    public boolean isMethodDelete() {
        return httpMethod.equals(HttpMethod.DELETE);
    }

    public boolean isMethodPost() {
        return httpMethod.equals(HttpMethod.POST);
    }

    public boolean isMethodPut() {
        return httpMethod.equals(HttpMethod.PUT);
    }

    public ByteBuf getBody() {
        return httpRequest.content();
    }

    public String getProtocol() {
        return DEFAULT_PROTOCOL;
    }

    public String getHost() {
        return httpHeaders.get(HttpHeaders.Names.HOST);
    }

    public String getPath() {
        return httpRequest.getUri();
    }

    public String getBaseUrl() {
        return getProtocol() + "://" + getHost();
    }

    public String getUrl() {
        return getBaseUrl() + getPath();
    }

    public String getNamedUrl(String resourceName)

    {
        return getNamedUrl(httpMethod, resourceName);
    }

    public String getNamedUrl(HttpMethod method, String resourceName) {
        Route route = routeResolver.getNamedRoute(resourceName, method);

        if (route != null) {
            return route.getFullPattern();
        }

        return null;
    }

    public <T> T readValue(Class<T> tClass) throws IOException {
        try (ByteBufInputStream inputStream = new ByteBufInputStream(httpRequest.content())) {
            return mapper.readValue(inputStream, tClass);
        }
    }

    private void determineEffectiveHttpMethod() {
        if (!HttpMethod.POST.equals(httpRequest.getMethod())) {
            return;
        }

        String methodString = httpHeaders.get(METHOD_TUNNEL);

        if (HttpMethod.PUT.name().equalsIgnoreCase(methodString) || HttpMethod.DELETE.name().equalsIgnoreCase(methodString)) {
            httpMethod = HttpMethod.valueOf(methodString.toUpperCase());
        }
    }

    private void parseQueryString() {
        if (!httpRequest.getUri().contains("?")) {
            return;
        }

        QueryStringDecoder decoder = new QueryStringDecoder(httpRequest.getUri());
        Map<String, List<String>> parameters = decoder.parameters();

        if (parameters == null || parameters.isEmpty()) {
            return;
        }

        queryStringMap = new HashMap<>(parameters.size());

        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            queryStringMap.put(entry.getKey(), entry.getValue().get(0));

            for (String value : entry.getValue()) {
                try {
                    httpHeaders.add(entry.getKey(), URLDecoder.decode(value, ENCODING));
                } catch (Exception e) {
                    httpHeaders.add(entry.getKey(), value);
                }
            }
        }
    }
}
