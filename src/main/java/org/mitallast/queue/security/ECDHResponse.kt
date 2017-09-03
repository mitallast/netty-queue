package org.mitallast.queue.security

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

class ECDHResponse(
    val sign: ByteArray,
    val encodedKey: ByteArray) : Message {

    companion object {
        val codec = Codec.of(
            ::ECDHResponse,
            ECDHResponse::sign,
            ECDHResponse::encodedKey,
            Codec.bytesCodec(),
            Codec.bytesCodec()
        )
    }
}
