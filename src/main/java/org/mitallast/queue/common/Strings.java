package org.mitallast.queue.common;

import java.nio.charset.Charset;

public class Strings {

    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static final String[] EMPTY_ARRAY = new String[0];

    public static String toCamelCase(String value) {
        return toCamelCase(value, null);
    }

    public static String toCamelCase(String value, StringBuilder sb) {
        boolean changed = false;
        char[] chars = value.toCharArray();
        for (int i = 0; i < value.length(); i++) {
            char c = chars[i];
            if (c == '_') {
                if (!changed) {
                    if (sb != null) {
                        sb.setLength(0);
                    } else {
                        sb = new StringBuilder();
                    }
                    // copy it over here
                    for (int j = 0; j < i; j++) {
                        sb.append(chars[i]);
                    }
                    changed = true;
                }
                sb.append(Character.toUpperCase(chars[++i]));
            } else {
                if (changed) {
                    sb.append(c);
                }
            }
        }
        if (!changed) {
            return value;
        }
        return sb.toString();
    }

    public static String format1Decimals(double value, String suffix) {
        String p = String.valueOf(value);
        int ix = p.indexOf('.') + 1;
        int ex = p.indexOf('E');
        char fraction = p.charAt(ix);
        if (fraction == '0') {
            if (ex != -1) {
                return p.substring(0, ix - 1) + p.substring(ex) + suffix;
            } else {
                return p.substring(0, ix - 1) + suffix;
            }
        } else {
            if (ex != -1) {
                return p.substring(0, ix) + fraction + p.substring(ex) + suffix;
            } else {
                return p.substring(0, ix) + fraction + suffix;
            }
        }
    }

    public static String[] splitStringByCommaToArray(final String s) {
        return splitStringToArray(s, ',');
    }

    public static String[] splitStringToArray(final String s, final char c) {
        if (s == null || s.length() == 0) {
            return Strings.EMPTY_ARRAY;
        }
        int count = 1;
        char[] chars = s.toCharArray();
        for (char cc : chars) {
            if (cc == c) {
                count++;
            }
        }
        final String[] result = new String[count];
        final StringBuilder builder = new StringBuilder();
        int res = 0;
        for (char aChar : chars) {
            if (aChar == c) {
                if (builder.length() > 0) {
                    result[res++] = builder.toString();
                    builder.setLength(0);
                }

            } else {
                builder.append(aChar);
            }
        }
        if (builder.length() > 0) {
            result[res++] = builder.toString();
        }
        if (res != count) {
            // we have empty strings, copy over to a new array
            String[] result1 = new String[res];
            System.arraycopy(result, 0, result1, 0, res);
            return result1;
        }
        return result;
    }

    public static boolean hasLength(CharSequence str) {
        return (str != null && str.length() > 0);
    }

    public static boolean substringMatch(CharSequence str, int index, CharSequence substring) {
        for (int j = 0; j < substring.length(); j++) {
            int i = index + j;
            if (i >= str.length() || str.charAt(i) != substring.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    public static String toString(CharSequence charSequence) {
        return charSequence == null ? null : charSequence.toString();
    }

    public static boolean isEmpty(String string) {
        return string == null || string.isEmpty();
    }

    public static boolean isEmpty(CharSequence string) {
        return string == null || string.length() == 0;
    }
}
