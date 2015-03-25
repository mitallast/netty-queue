package org.mitallast.queue.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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

    public static String[] splitStringToArray(final String s, final char delimiter) {
        if (s == null || s.isEmpty()) {
            return Strings.EMPTY_ARRAY;
        }
        int length = s.length();
        List<String> list = new ArrayList<>(8);
        int start = 0;
        for (int index = 0; index < length; index++) {
            char currentChar = s.charAt(index);
            if (currentChar == delimiter) {
                list.add(s.substring(start, index));
                start = index + 1;
            }
        }
        if (start < length) {
            list.add(s.substring(start, length));
        }
        return list.toArray(new String[list.size()]);
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

    public static ByteBuf toByteBuf(String string) {
        ByteBuf buffer = Unpooled.buffer(string.length() * 3);
        ByteBufUtil.writeUtf8(buffer, string);
        return buffer;
    }

    public static boolean isEmpty(String string) {
        return string == null || string.isEmpty();
    }

    public static boolean isEmpty(CharSequence string) {
        return string == null || string.length() == 0;
    }
}
