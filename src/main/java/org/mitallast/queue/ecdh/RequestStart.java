package org.mitallast.queue.ecdh;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

import java.util.Arrays;

import static javax.xml.bind.DatatypeConverter.printHexBinary;

public class RequestStart implements Message {
    public static final Codec<RequestStart> codec = Codec.of(
            RequestStart::new,
            RequestStart::publicKey,
            Codec.bytesCodec
    );

    private final byte[] publicKey;

    public RequestStart(byte[] publicKey) {
        this.publicKey = publicKey;
    }

    public byte[] publicKey() {
        return publicKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RequestStart that = (RequestStart) o;

        return Arrays.equals(publicKey, that.publicKey);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(publicKey);
    }

    @Override
    public String toString() {
        return "RequestStart{publicKey=" + printHexBinary(publicKey) + '}';
    }
}
