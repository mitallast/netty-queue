package org.mitallast.transport.http.route;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.transport.MethodNotAllowedException;
import org.mitallast.transport.NotFoundException;
import org.mitallast.transport.Request;

import java.util.List;

public class RouteResolver {
    private RouteMapping routeMapping;

    public RouteResolver(RouteMapping routes) {
        super();
        this.routeMapping = routes;
    }

    public Route getNamedRoute(String name, HttpMethod method) {
        return routeMapping.getNamedRoute(name, method);
    }

    public Action resolve(Request request) {
        Action action = routeMapping.getActionFor(request.getHttpMethod(), request.getPath());

        if (action != null) {
            return action;
        }

        List<HttpMethod> allowedMethods = routeMapping.getAllowedMethods(request.getPath());

        if (allowedMethods != null && !allowedMethods.isEmpty()) {
            throw new MethodNotAllowedException(request.getUrl(), allowedMethods);
        }

        throw new NotFoundException("Unresolvable URL: " + request.getUrl());
    }
}
