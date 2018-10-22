package org.mitallast.queue.common;

public final class Hex {
    private final static String hex = "0123456789abcdef";
    private final static String[] table = new String[0xFF];

    public static byte[] parseHexBinary(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public static String printHexBinary(byte[] a) {
        var builder = new StringBuilder(a.length * 2);
        for (byte b : a) {
            builder.append(hex.charAt((b >> 4) & 0xF));
            builder.append(hex.charAt(b & 0xF));
        }
        return builder.toString();
    }
}
