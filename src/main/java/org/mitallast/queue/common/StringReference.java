package org.mitallast.queue.common;

import java.util.ArrayList;
import java.util.List;

public class StringReference implements CharSequence {

    public static final CharSequence[] EMPTY = new CharSequence[0];
    private final CharSequence sequence;
    private final int start;
    private final int end;
    private int hash;

    public StringReference(CharSequence sequence) {
        this(sequence, 0, sequence.length());
    }

    public StringReference(CharSequence sequence, int start, int end) {
        this.sequence = sequence;
        this.start = start;
        this.end = end;
    }

    public static CharSequence[] splitStringToArray(final CharSequence s, final char delimiter) {
        if (s == null || s.length() == 0) {
            return EMPTY;
        }
        int length = s.length();
        List<CharSequence> list = new ArrayList<>(8);
        int start = 0;
        for (int index = 0; index < length; index++) {
            char currentChar = s.charAt(index);
            if (currentChar == delimiter) {
                if (start < index) {
                    list.add(subSequence(s, start, index));
                }
                start = index + 1;
            }
        }
        if (start < length) {
            list.add(subSequence(s, start, length));
        }
        return list.toArray(new CharSequence[list.size()]);
    }

    public static CharSequence subSequence(CharSequence sequence, int start, int end) {
        if (sequence instanceof StringReference) {
            return sequence.subSequence(start, end);
        } else {
            return new StringReference(sequence, start, end);
        }
    }

    public static boolean equals(CharSequence a, CharSequence b) {
        if (a == b) {
            return true;
        }
        if (a.length() != b.length()) {
            return false;
        }
        for (int i = 0; i < a.length(); i++) {
            if (a.charAt(i) != b.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static StringReference of(CharSequence sequence) {
        if (sequence instanceof StringReference) {
            return (StringReference) sequence;
        } else {
            return new StringReference(sequence);
        }
    }

    @Override
    public int length() {
        return end - start;
    }

    @Override
    public char charAt(int index) {
        return sequence.charAt(start + index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new StringReference(sequence, this.start + start, this.start + end);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof CharSequence)) return false;

        CharSequence that = (CharSequence) o;
        return equals(this, that);
    }

    @Override
    public int hashCode() {
        int hash = this.hash;
        if (hash != 0 || length() == 0) {
            return hash;
        }

        for (int i = start; i < end; i++) {
            hash = hash * 31 ^ sequence.charAt(i) & 31;
        }

        return this.hash = hash;
    }

    @Override
    public String toString() {
        int len = length();
        char[] buffer = new char[len];
        for (int i = start, j = 0; i < end; i++, j++) {
            buffer[j] = sequence.charAt(i);
        }
        return String.valueOf(buffer);
    }
}
