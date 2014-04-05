package org.mitallast.transport.http.route.regex;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.transport.http.route.Route;
import org.mitallast.transport.http.url.UrlMatcher;
import org.mitallast.transport.http.url.UrlRegex;

import java.lang.reflect.Method;
import java.util.Map;

public class RegexRoute extends Route {

    public RegexRoute(UrlMatcher urlMatcher, HttpMethod method, Object controller, Method action, String name, Map<String, Object> parameters, String baseUrl) {
        super(urlMatcher, method, controller, action, name, parameters, baseUrl);
    }

    public RegexRoute(String urlPattern, HttpMethod method, Object controller, Method action, String name, Map<String, Object> parameters, String baseUrl) {
        super(new UrlRegex(urlPattern), method, controller, action, name, parameters, baseUrl);
    }
}
