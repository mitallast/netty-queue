package org.mitallast.queue.transport.netty

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder
import net.jpountz.lz4.LZ4Factory
import org.mitallast.queue.common.Hex
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.security.ECDHEncrypted
import org.mitallast.queue.security.ECDHFlow
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.nio.ByteBuffer
import java.security.MessageDigest
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.IvParameterSpec

class ECDHCodecDecoder : MessageToMessageDecoder<Message>() {
    private val decompressor = LZ4Factory.fastestInstance().fastDecompressor()
    private val cipher: Cipher = Cipher.getInstance(ECDHFlow.AES256)
    private val hmac = Mac.getInstance(ECDHFlow.HmacSHA256)

    private val sign = ByteArray(hmac.macLength)
    private var decrypted = ByteArray(4096)
    private var decryptedBuffer = ByteBuffer.wrap(decrypted)
    private var decompressed = ByteArray(4096)
    private var input = ByteArrayInputStream(decompressed)
    private var stream = DataInputStream(input)

    override fun decode(ctx: ChannelHandlerContext, msg: Message, out: MutableList<Any>) {
        if (msg is ECDHEncrypted) {
            try {
                val ecdhFlow = ctx.channel().attr(ECDHFlow.key).get()
                val secretKey = ecdhFlow.secretKey()
                val buffer = msg.encrypted.nioBuffer()
                buffer.mark()

                // verify
                hmac.init(secretKey)
                hmac.update(msg.iv)
                hmac.update(buffer)
                hmac.doFinal(sign, 0)
                if (!MessageDigest.isEqual(msg.sign, sign)) {
                    throw IllegalArgumentException("not verified")
                }

                // decrypt
                buffer.reset()
                cipher.init(Cipher.DECRYPT_MODE, secretKey, IvParameterSpec(msg.iv))
                val maxDecryptedLen = cipher.getOutputSize(msg.encrypted.readableBytes())
                if (decrypted.size < maxDecryptedLen) {
                    decrypted = ByteArray(maxDecryptedLen + 4096)
                    decryptedBuffer = ByteBuffer.wrap(decrypted)
                } else {
                    decryptedBuffer.clear()
                }
                cipher.doFinal(buffer, decryptedBuffer)

                // decompress
                if (decompressed.size < msg.len) {
                    decompressed = ByteArray(msg.len + 4096)
                    input = ByteArrayInputStream(decompressed)
                    stream = DataInputStream(input)
                }
                input.reset()
                decompressor.decompress(decrypted, decompressed, msg.len)

                // decode list
                val count = stream.readInt()
                for (i in 0 until count) {
                    out.add(Codec.anyCodec<Message>().read(stream))
                }
            } finally {
                msg.encrypted.release()
            }
        } else {
            out.add(msg)
        }
    }
}
