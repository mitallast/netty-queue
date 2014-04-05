package org.mitallast.transport.http.route.regex;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.transport.http.route.Route;
import org.mitallast.transport.http.route.RouteBuilder;

import java.lang.reflect.Method;
import java.util.Map;

public class RegexRouteBuilder extends RouteBuilder {

    public RegexRouteBuilder(String uri, Object controller) {
        super(uri, controller);
    }

    @Override
    protected Route newRoute(String pattern, HttpMethod method, Object controller, Method action, String name, Map<String, Object> parameters, String baseUrl) {
        return new RegexRoute(pattern, method, controller, action, name, parameters, baseUrl);
    }

    @Override
    protected String toRegexPattern(String uri) {
        return uri;
    }
}
