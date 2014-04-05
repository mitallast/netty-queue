package org.mitallast.transport.http.route.parametrized;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.transport.http.route.Route;
import org.mitallast.transport.http.url.UrlMatch;
import org.mitallast.transport.http.url.UrlPattern;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class ParametrizedRoute extends Route {

    private UrlPattern[] aliases;

    public ParametrizedRoute(UrlPattern urlMatcher, HttpMethod method, Object controller, Method action, String name, Map<String, Object> parameters, String baseUrl) {
        super(urlMatcher, method, controller, action, name, parameters, baseUrl);
    }

    public ParametrizedRoute(String urlPattern, HttpMethod method, Object controller, Method action, String name, Map<String, Object> parameters, String baseUrl) {
        super(new UrlPattern(urlPattern), method, controller, action, name, parameters, baseUrl);
    }

    public void addAliases(List<String> uris) {
        if (uris == null) {
            return;
        }

        aliases = new UrlPattern[uris.size()];
        int i = 0;

        for (String uri : uris) {
            aliases[i++] = new UrlPattern(uri);
        }
    }

    @Override
    public UrlMatch match(String url) {
        UrlMatch match = super.match(url);

        if (match == null && aliases != null) {
            for (UrlPattern alias : aliases) {
                match = alias.match(url);

                if (match != null) {
                    break;
                }
            }
        }

        return match;
    }
}
