package org.mitallast;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.index.IndexController;
import org.mitallast.transport.http.HttpServer;
import org.mitallast.transport.http.route.RouteMapping;
import org.mitallast.transport.http.route.RouteResolver;
import org.mitallast.transport.http.route.parametrized.ParametrizedRouteBuilder;

import java.io.IOException;

public class Main {
    public static void main(String... args) throws IOException, InterruptedException {
        RouteMapping routeMapping = new RouteMapping();

        routeMapping.addRoute(
                new ParametrizedRouteBuilder("/", new IndexController())
                    .action("indexAction", HttpMethod.GET)
                    .build()
        );

        RouteResolver routeResolver = new RouteResolver(routeMapping);
        new HttpServer(8080, routeResolver).run();
    }
}
