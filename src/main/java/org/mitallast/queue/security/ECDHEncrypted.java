package org.mitallast.queue.security;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class ECDHEncrypted implements Message {
    public static final Codec<ECDHEncrypted> codec = Codec.Companion.of(
            ECDHEncrypted::new,
            ECDHEncrypted::sign,
            ECDHEncrypted::iv,
            ECDHEncrypted::encrypted,
            Codec.Companion.bytesCodec(),
            Codec.Companion.bytesCodec(),
            Codec.Companion.bytesCodec()
    );

    private final byte[] sign;
    private final byte[] iv;
    private final byte[] encrypted;

    public ECDHEncrypted(byte[] sign, byte[] iv, byte[] encrypted) {
        this.sign = sign;
        this.iv = iv;
        this.encrypted = encrypted;
    }

    public byte[] sign() {
        return sign;
    }

    public byte[] iv() {
        return iv;
    }

    public byte[] encrypted() {
        return encrypted;
    }
}
