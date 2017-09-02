package org.mitallast.queue.security;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

import static javax.xml.bind.DatatypeConverter.printHexBinary;

public class ECDHRequest implements Message {
    public static final Codec<ECDHRequest> codec = Codec.of(
            ECDHRequest::new,
            ECDHRequest::sign,
            ECDHRequest::encodedKey,
            Codec.bytesCodec,
            Codec.bytesCodec
    );

    private final byte[] sign;
    private final byte[] encodedKey;

    public ECDHRequest(byte[] sign, byte[] encodedKey) {
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
        return "ECDHRequest{" +
                "sign=" + printHexBinary(sign) +
                ", encodedKey=" + printHexBinary(encodedKey) +
                '}';
    }
}
