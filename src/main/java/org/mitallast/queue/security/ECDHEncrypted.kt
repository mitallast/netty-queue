package org.mitallast.queue.security

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

class ECDHEncrypted(
    val sign: ByteArray,
    val iv: ByteArray,
    val encrypted: ByteArray) : Message {

    companion object {
        val codec = Codec.of(
            ::ECDHEncrypted,
            ECDHEncrypted::sign,
            ECDHEncrypted::iv,
            ECDHEncrypted::encrypted,
            Codec.bytesCodec(),
            Codec.bytesCodec(),
            Codec.bytesCodec()
        )
    }
}
