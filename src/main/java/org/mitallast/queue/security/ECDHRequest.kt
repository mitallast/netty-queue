package org.mitallast.queue.security

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

class ECDHRequest(
    val sign: ByteArray,
    val encodedKey: ByteArray) : Message {

    companion object {
        val codec = Codec.of(
            ::ECDHRequest,
            ECDHRequest::sign,
            ECDHRequest::encodedKey,
            Codec.bytesCodec(),
            Codec.bytesCodec()
        )
    }
}
