package org.mitallast.queue.security;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

import static javax.xml.bind.DatatypeConverter.printHexBinary;

public class ECDHResponse implements Message {
    public static final Codec<ECDHResponse> codec = Codec.of(
            ECDHResponse::new,
            ECDHResponse::sign,
            ECDHResponse::encodedKey,
            Codec.bytesCodec,
            Codec.bytesCodec
    );

    private final byte[] sign;
    private final byte[] encodedKey;

    public ECDHResponse(byte[] sign, byte[] encodedKey) {
        this.sign = sign;
        this.encodedKey = encodedKey;
    }

    public byte[] sign() {
        return sign;
    }

    public byte[] encodedKey() {
        return encodedKey;
    }

    @Override
    public String toString() {
        return "ECDHResponse{" +
                "sign=" + printHexBinary(sign) +
                ", encodedKey=" + printHexBinary(encodedKey) +
                '}';
    }
}
