package org.mitallast.queue.crdt.protocol

import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message

data class AppendSuccessful(
    val bucket: Int,
    val replica: Long,
    val index: Long) : Message {

    companion object {
        val codec = Codec.of(
            ::AppendSuccessful,
            AppendSuccessful::bucket,
            AppendSuccessful::replica,
            AppendSuccessful::index,
            Codec.intCodec(),
            Codec.longCodec(),
            Codec.longCodec()
        )
    }
}
