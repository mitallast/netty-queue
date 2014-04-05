package org.mitallast.transport.http.url;

import java.util.List;

public interface UrlMatcher {
    public boolean matches(String url);

    public UrlMatch match(String url);

    public String getPattern();

    public List<String> getParameterNames();
}