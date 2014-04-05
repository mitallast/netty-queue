package org.mitallast.transport.http.url;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author toddf
 * @since Jan 7, 2011
 */
public class UrlRegex implements UrlMatcher {
    public static final String PARAMETER_PREFIX = "regexGroup";

    private Pattern pattern;

    public UrlRegex(String regex) {
        this(Pattern.compile(regex));
    }

    public UrlRegex(Pattern pattern) {
        super();
        setPattern(pattern);
    }

    public String getPattern() {
        return pattern.pattern();
    }

    private void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public List<String> getParameterNames() {
        return null;
    }

    @Override
    public boolean matches(String url) {
        return (match(url) != null);
    }

    @Override
    public UrlMatch match(String url) {
        Matcher matcher = pattern.matcher(url);

        if (matcher.matches()) {
            return new UrlMatch(extractParameters(matcher));
        }

        return null;
    }

    /**
     * Extracts parameter values from a Matcher instance.
     *
     * @param matcher matcher
     * @return a Map containing parameter values indexed by their corresponding parameter name.
     */
    private Map<String, String> extractParameters(Matcher matcher) {
        Map<String, String> values = new HashMap<String, String>();

        for (int i = 0; i < matcher.groupCount(); i++) {
            String value = matcher.group(i + 1);

            if (value != null) {
                values.put(PARAMETER_PREFIX + i, value);
            }
        }

        return values;
    }
}
