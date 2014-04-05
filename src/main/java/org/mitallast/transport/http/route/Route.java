package org.mitallast.transport.http.route;

import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.transport.Request;
import org.mitallast.transport.Response;
import org.mitallast.transport.ServiceException;
import org.mitallast.transport.http.url.UrlMatch;
import org.mitallast.transport.http.url.UrlMatcher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class Route {

    private UrlMatcher urlMatcher;
    private Object controller;
    private Method action;
    private HttpMethod method;

    private String name;
    private String baseUrl;
    private Map<String, Object> parameters = new HashMap<>();

    public Route(UrlMatcher urlMatcher, HttpMethod method, Object controller, Method action, String name, Map<String, Object> parameters, String baseUrl) {
        this.urlMatcher = urlMatcher;
        this.controller = controller;
        this.action = action;
        this.method = method;
        this.name = name;
        this.baseUrl = baseUrl;
        this.parameters.putAll(parameters);
    }

    public boolean hasParameter(String name) {
        return (getParameter(name) != null);
    }

    public Object getParameter(String name) {
        return parameters.get(name);
    }

    public UrlMatch match(String url) {
        return urlMatcher.match(url);
    }

    public UrlMatcher getUrlMatcher() {
        return urlMatcher;
    }

    public Object getController() {
        return controller;
    }

    public Method getAction() {
        return action;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public String getName() {
        return name;
    }

    public boolean hasName() {
        return (getName() != null && !getName().trim().isEmpty());
    }

    public String getPattern() {
        return urlMatcher.getPattern();
    }

    public String getBaseUrl() {
        return (baseUrl == null ? "" : baseUrl);
    }

    public String getFullPattern() {
        return getBaseUrl() + getPattern();
    }

    public Object invoke(Request request, Response response)
    {
        try
        {
            return action.invoke(controller, request, response);
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();

            if (RuntimeException.class.isAssignableFrom(cause.getClass()))
            {
                throw (RuntimeException) e.getCause();
            }
            else
            {
                throw new RuntimeException(cause);
            }
        }
        catch (Exception e)
        {
            throw new ServiceException(e);
        }
    }
}
