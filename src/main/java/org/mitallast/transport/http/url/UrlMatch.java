package org.mitallast.transport.http.url;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Contains the results of a UrlPattern.match() call, reflecting the match outcome
 * and containing any parameter values, if applicable.
 * <p/>
 * <p/>UrlMatch is immutable.
 *
 * @author toddf
 * @since Apr 29, 2010
 */
public class UrlMatch {
    /**
     * Parameter values parsed from the URL during the match.
     */
    private Map<String, String> parameters = new HashMap<String, String>();


    // SECTION: CONSTRUCTOR

    public UrlMatch(Map<String, String> parameters) {
        super();

        if (parameters != null) {
            this.parameters.putAll(parameters);
        }
    }


    // SECTION: ACCESSORS

    /**
     * Retrieves a parameter value parsed from the URL during the match.
     *
     * @param name the name of a parameter for which to retrieve the value.
     * @return the parameter value from the URL, or null if not present.
     */
    public String get(String name) {
        return parameters.get(name);
    }

    /**
     * Retrieves the parameter entries as a set.
     *
     * @return a Set of Map entries (by String, String).
     */
    public Set<Map.Entry<String, String>> parameterSet() {
        return Collections.unmodifiableSet(parameters.entrySet());
    }
}
