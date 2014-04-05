package org.mitallast.transport.http.route;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.transport.http.url.UrlMatch;

import java.util.*;

public class RouteMapping {

    private Map<HttpMethod, List<Route>> routes;

    private List<Route> getRoutes = new ArrayList<Route>();
    private List<Route> postRoutes = new ArrayList<Route>();
    private List<Route> putRoutes = new ArrayList<Route>();
    private List<Route> deleteRoutes = new ArrayList<Route>();

    private List<Route> headRoutes = new ArrayList<Route>();
    private List<Route> optionRoutes = new ArrayList<Route>();

    private Map<String, Map<HttpMethod, Route>> routesByName = new HashMap<String, Map<HttpMethod, Route>>();
    private Map<String, List<Route>> routesByPattern = new LinkedHashMap<String, List<Route>>();

    public RouteMapping() {
        routes = new HashMap<HttpMethod, List<Route>>();
        routes.put(HttpMethod.DELETE, deleteRoutes);
        routes.put(HttpMethod.GET, getRoutes);
        routes.put(HttpMethod.POST, postRoutes);
        routes.put(HttpMethod.PUT, putRoutes);
        routes.put(HttpMethod.HEAD, headRoutes);
        routes.put(HttpMethod.OPTIONS, optionRoutes);
    }

    public List<Route> getRoutesFor(HttpMethod method) {
        List<Route> routesFor = routes.get(method);
        if (routesFor == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(routesFor);
    }

    public Action getActionFor(HttpMethod method, String path) {
        for (Route route : routes.get(method)) {
            UrlMatch match = route.match(path);
            if (match != null) {
                return new Action(route, match);
            }
        }
        return null;
    }

    public List<Route> getMatchingRoutes(String path) {
        for (List<Route> patternRoutes : routesByPattern.values()) {
            if (patternRoutes.get(0).match(path) != null) {
                return Collections.unmodifiableList(patternRoutes);
            }
        }

        return Collections.emptyList();
    }

    public List<HttpMethod> getAllowedMethods(String path) {
        List<Route> matchingRoutes = getMatchingRoutes(path);

        if (matchingRoutes.isEmpty()) {
            return Collections.emptyList();
        }

        List<HttpMethod> methods = new ArrayList<HttpMethod>();

        for (Route route : matchingRoutes) {
            methods.add(route.getMethod());
        }

        return methods;
    }

    public Route getNamedRoute(String name, HttpMethod method) {
        Map<HttpMethod, Route> routesByMethod = routesByName.get(name);

        if (routesByMethod == null) {
            return null;
        }

        return routesByMethod.get(method);
    }

    public void addRoute(List<Route> routes) {
        for(Route route : routes) {
            addRoute(route);
        }
    }

    public void addRoute(Route route) {
        routes.get(route.getMethod()).add(route);
        addByPattern(route);

        if (route.hasName()) {
            addNamedRoute(route);
        }
    }

    private void addNamedRoute(Route route) {
        Map<HttpMethod, Route> routesByMethod = routesByName.get(route.getName());

        if (routesByMethod == null) {
            routesByMethod = new HashMap<HttpMethod, Route>();
            routesByName.put(route.getName(), routesByMethod);
        }

        routesByMethod.put(route.getMethod(), route);
    }

    private void addByPattern(Route route) {
        List<Route> urlRoutes = routesByPattern.get(route.getPattern());

        if (urlRoutes == null) {
            urlRoutes = new ArrayList<Route>();
            routesByPattern.put(route.getPattern(), urlRoutes);
        }

        urlRoutes.add(route);
    }
}
