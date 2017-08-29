package org.mitallast.queue.ecdh;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

import static javax.xml.bind.DatatypeConverter.printHexBinary;

public class Encrypted implements Message {
    public static final Codec<Encrypted> codec = Codec.of(
            Encrypted::new,
//            Encrypted::sign,
            Encrypted::iv,
            Encrypted::encrypted,
//            Codec.bytesCodec,
            Codec.bytesCodec,
            Codec.bytesCodec
    );

//    private final byte[] sign;
    private final byte[] iv;
    private final byte[] encrypted;

    public Encrypted(byte[] iv, byte[] encrypted) {
//        this.sign = sign;
        this.iv = iv;
        this.encrypted = encrypted;
    }

//    public byte[] sign() {
//        return sign;
//    }

    public byte[] iv() {
        return iv;
    }

    public byte[] encrypted() {
        return encrypted;
    }

    @Override
    public String toString() {
        return "Encrypted{" +
//                "sign=" + printHexBinary(sign) +
                "iv=" + printHexBinary(iv) +
                ", encrypted=" + printHexBinary(encrypted) +
                '}';
    }
}
