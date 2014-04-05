package org.mitallast.transport.http.route.parametrized;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.transport.http.route.Route;
import org.mitallast.transport.http.route.RouteBuilder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParametrizedRouteBuilder extends RouteBuilder {

    private List<String> aliases = new ArrayList<>();

    public ParametrizedRouteBuilder(String uri, Object controller) {
        super(uri, controller);
    }

    @Override
    protected Route newRoute(String pattern, HttpMethod method, Object controller, Method action, String name, Map<String, Object> parameters, String baseUrl) {
        ParametrizedRoute r = new ParametrizedRoute(pattern, method, controller, action, name, parameters, baseUrl);
        r.addAliases(aliases);
        return r;
    }

    public ParametrizedRouteBuilder alias(String uri) {
        if (!aliases.contains(uri)) {
            aliases.add(uri);
        }

        return this;
    }

    @Override
    protected String toRegexPattern(String uri) {
        String pattern = uri;

        if (pattern != null && !pattern.startsWith("/")) {
            pattern = "/" + pattern;
        }

        return pattern;
    }
}
