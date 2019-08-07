package org.mitallast.queue.transport.netty

import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder
import net.jpountz.lz4.LZ4Factory
import org.apache.logging.log4j.LogManager
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.security.ECDHEncrypted
import org.mitallast.queue.security.ECDHFlow
import org.mitallast.queue.security.ECDHRequest
import org.mitallast.queue.security.ECDHResponse
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.IvParameterSpec

private class UnsafeByteArrayOutputStream(size: Int) : ByteArrayOutputStream(size) {
    fun array(): ByteArray = buf
}

class ECDHCodecEncoder : MessageToMessageEncoder<Message>() {
    private val logger = LogManager.getLogger()

    private val output = UnsafeByteArrayOutputStream(4096)
    private val stream = DataOutputStream(output)
    private val compressor = LZ4Factory.fastestInstance().fastCompressor()
    private val cipher: Cipher = Cipher.getInstance(ECDHFlow.AES256)
    private val hmac = Mac.getInstance(ECDHFlow.HmacSHA256)

    private var compressed = ByteArray(4096)
    private var encrypted = ByteArray(4096)
    private var messages = ArrayList<Message>(4096)

    override fun encode(ctx: ChannelHandlerContext, msg: Message, out: MutableList<Any>) {
        when (msg) {
            is ECDHRequest -> out.add(msg)
            is ECDHResponse -> out.add(msg)
            is ECDHEncrypted -> out.add(msg)
            else -> {
                messages.add(msg)
                if (messages.size >= 4096) {
                    out.add(encrypt(ctx))
                } else {
                    out.add(Unpooled.EMPTY_BUFFER)
                }
            }
        }
    }

    override fun flush(ctx: ChannelHandlerContext) {
        if (messages.isNotEmpty()) {
            val encrypted = encrypt(ctx)
            ctx.write(encrypted, ctx.voidPromise())
        }
        ctx.flush()
    }

    private fun encrypt(ctx: ChannelHandlerContext): ECDHEncrypted {
        // encode list
        logger.info("encrypt ${messages.size} messages")
        output.reset()
        stream.writeInt(messages.size)
        for (message in messages) {
            Codec.anyCodec<Message>().write(stream, message)
        }
        messages.clear()

        val data = output.array()
        val len = output.size()

        // compress
        val maxCompressedLength = compressor.maxCompressedLength(len)
        if (compressed.size < maxCompressedLength) {
            compressed = ByteArray(maxCompressedLength + 4096)
        }
        val compressedLen = compressor.compress(data, 0, len, compressed, 0)

        // encrypt
        val ecdhFlow = ctx.channel().attr(ECDHFlow.key).get()
        val secretKey = ecdhFlow.secretKey()
        cipher.init(Cipher.ENCRYPT_MODE, secretKey)
        val params = cipher.parameters
        val iv = params.getParameterSpec(IvParameterSpec::class.java).iv
        val encryptedMaxLen = cipher.getOutputSize(compressedLen)
        if (encrypted.size < encryptedMaxLen) {
            encrypted = ByteArray(encryptedMaxLen + 4096)
        }
        val encryptedLen = cipher.doFinal(compressed, 0, compressedLen, encrypted)

        // sign
        hmac.init(secretKey)
        hmac.update(iv)
        hmac.update(encrypted, 0, encryptedLen)
        val sign = hmac.doFinal()

        return ECDHEncrypted(sign, iv, len, Unpooled.wrappedBuffer(encrypted, 0, encryptedLen))
    }
}
