package org.mitallast.queue.common.strings;

import java.util.ArrayList;
import java.util.List;

public class Strings {

    public static final String[] EMPTY_ARRAY = new String[0];

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
                if (start < index) {
                    list.add(s.substring(start, index));
                }
                start = index + 1;
            }
        }
        if (start < length) {
            list.add(s.substring(start, length));
        }
        return list.toArray(new String[list.size()]);
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
