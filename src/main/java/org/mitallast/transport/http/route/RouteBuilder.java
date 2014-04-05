package org.mitallast.transport.http.route;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.ConfigurationException;
import org.mitallast.transport.Request;
import org.mitallast.transport.Response;

import java.lang.reflect.Method;
import java.util.*;

import static io.netty.handler.codec.http.HttpMethod.*;

public abstract class RouteBuilder {
    static final String DELETE_ACTION_NAME = "delete";
    static final String GET_ACTION_NAME = "read";
    static final String POST_ACTION_NAME = "create";
    static final String PUT_ACTION_NAME = "update";
    static final String HEAD_ACTION_NAME = "headers";
    static final String OPTION_ACTION_NAME = "options";

    static final List<HttpMethod> DEFAULT_HTTP_METHODS = Arrays.asList(GET, POST, PUT, DELETE);
    static final Map<HttpMethod, String> ACTION_MAPPING = new HashMap<HttpMethod, String>();

    static {
        ACTION_MAPPING.put(DELETE, DELETE_ACTION_NAME);
        ACTION_MAPPING.put(GET, GET_ACTION_NAME);
        ACTION_MAPPING.put(POST, POST_ACTION_NAME);
        ACTION_MAPPING.put(PUT, PUT_ACTION_NAME);
        ACTION_MAPPING.put(HEAD, HEAD_ACTION_NAME);
        ACTION_MAPPING.put(OPTIONS, OPTION_ACTION_NAME);
    }

    private String uri;
    private List<HttpMethod> methods = new ArrayList<HttpMethod>();
    private Map<HttpMethod, String> actionNames = new HashMap<HttpMethod, String>();
    private Object controller;
    private String name;
    private String baseUrl;
    private Map<String, Object> parameters = new HashMap<String, Object>();


    public RouteBuilder(String uri, Object controller) {
        this.uri = uri;
        this.controller = controller;
    }

    public RouteBuilder action(String action, HttpMethod method) {
        if (!actionNames.containsKey(method)) {
            actionNames.put(method, action);
        }
        if (!methods.contains(method)) {
            methods.add(method);
        }
        return this;
    }

    public RouteBuilder baseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
        return this;
    }

    public RouteBuilder method(HttpMethod... methods) {
        for (HttpMethod method : methods) {
            if (!this.methods.contains(method)) {
                this.methods.add(method);
            }
        }

        return this;
    }

    public RouteBuilder name(String name) {
        this.name = name;
        return this;
    }

    public RouteBuilder parameter(String name, Object value) {
        parameters.put(name, value);
        return this;
    }

    public List<Route> build() {
        if (methods.isEmpty()) {
            methods = DEFAULT_HTTP_METHODS;
        }
        List<Route> routes = new ArrayList<>();
        String pattern = toRegexPattern(uri);

        for (HttpMethod method : methods) {
            String actionName = actionNames.get(method);
            if (actionName == null) {
                actionName = ACTION_MAPPING.get(method);
            }

            Method action = determineActionMethod(controller, actionName);
            routes.add(newRoute(pattern, method, controller, action, name, parameters, baseUrl));
        }

        return routes;
    }

    private Method determineActionMethod(Object controller, String actionName) {
        try {
            return controller.getClass().getMethod(actionName, Request.class, Response.class);
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    protected abstract Route newRoute(String pattern, HttpMethod method, Object controller, Method action, String name, Map<String, Object> parameters, String baseUrl);

    protected abstract String toRegexPattern(String uri);
}
