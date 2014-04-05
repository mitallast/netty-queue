package org.mitallast.transport.http.route;

import org.mitallast.transport.Request;
import org.mitallast.transport.Response;
import org.mitallast.transport.http.url.UrlMatch;

public class Action {
    private Route route;
    private UrlMatch match;

    public Action(Route route, UrlMatch match) {
        this.route = route;
        this.match = match;
    }

    public Object invoke(Request request, Response response)
    {
        return route.invoke(request, response);
    }
}
