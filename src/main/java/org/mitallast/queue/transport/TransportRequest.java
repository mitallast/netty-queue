package org.mitallast.queue.transport;

import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.mitallast.queue.rest.RestRequest;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportRequest implements RestRequest {

    public static final String ENCODING = "UTF-8";
    public static final Charset charset = Charset.forName(ENCODING);

    public static final String METHOD_TUNNEL = "_method";
    private static final String DEFAULT_PROTOCOL = "http";
    private FullHttpRequest httpRequest;
    private HttpMethod httpMethod;
    private HttpHeaders httpHeaders;

    private Map<String, List<String>> paramMap;
    private String queryPath;

    public TransportRequest(FullHttpRequest request) {
        this.httpRequest = request;
        this.httpMethod = request.getMethod();
        this.httpHeaders = request.headers();
        this.parseQueryString();
        determineEffectiveHttpMethod();
    }

    @Override
    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

    @Override
    public Map<String, List<String>> getParamMap() {
        return paramMap;
    }

    @Override
    public String param(String param) {
        List<String> params = paramMap.get(param);
        return (params == null || params.isEmpty())
            ? null
            : params.get(0);
    }

    @Override
    public boolean hasParam(String param) {
        return paramMap.containsKey(param);
    }

    @Override
    public HttpHeaders getHttpHeaders() {
        return httpHeaders;
    }

    @Override
    public boolean isMethodGet() {
        return httpMethod.equals(HttpMethod.GET);
    }

    @Override
    public boolean isMethodDelete() {
        return httpMethod.equals(HttpMethod.DELETE);
    }

    @Override
    public boolean isMethodPost() {
        return httpMethod.equals(HttpMethod.POST);
    }

    @Override
    public boolean isMethodPut() {
        return httpMethod.equals(HttpMethod.PUT);
    }

    @Override
    public boolean isMethodHead() {
        return httpMethod.equals(HttpMethod.HEAD);
    }

    @Override
    public boolean isMethodOptions() {
        return httpMethod.equals(HttpMethod.OPTIONS);
    }

    @Override
    public InputStream getInputStream() {
        return new ByteBufInputStream(httpRequest.content());
    }

    @Override
    public String getBody() {
        return httpRequest.content().toString(charset);
    }

    @Override
    public String getProtocol() {
        return DEFAULT_PROTOCOL;
    }

    @Override
    public String getHost() {
        return httpHeaders.get(HttpHeaders.Names.HOST);
    }

    @Override
    public String getQueryPath() {
        return queryPath;
    }

    @Override
    public String getBaseUrl() {
        return getProtocol() + "://" + getHost();
    }

    @Override
    public String getUrl() {
        return getBaseUrl() + getQueryPath();
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

        String uri = httpRequest.getUri();
        if (!uri.contains("?")) {
            paramMap = new HashMap<>();
            queryPath = uri;
            return;
        }

        QueryStringDecoder decoder = new QueryStringDecoder(uri);
        queryPath = decoder.path();
        paramMap = decoder.parameters();
        if (paramMap == Collections.<String, List<String>>emptyMap()) {
            paramMap = new HashMap<>();
        }
    }
}
