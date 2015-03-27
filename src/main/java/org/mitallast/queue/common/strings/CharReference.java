package org.mitallast.queue.common.strings;

public class CharReference implements CharSequence {

    private final char[] ref;
    private final int start;
    private final int end;

    public CharReference(char[] ref, int start, int end) {
        this.ref = ref;
        this.start = start;
        this.end = end;
    }

    @Override
    public int length() {
        return end - start;
    }

    @Override
    public char charAt(int index) {
        return ref[start + index];
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new CharReference(ref, this.start + start, this.start + end);
    }

    @Override
    public String toString() {
        return new String(ref, start, end);
    }
}
