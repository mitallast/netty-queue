package org.mitallast.queue.transport.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.security.ECDHEncrypted;
import org.mitallast.queue.security.ECDHFlow;
import org.mitallast.queue.security.ECDHRequest;
import org.mitallast.queue.security.ECDHResponse;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;

public class ECDHNewEncoder extends ChannelOutboundHandlerAdapter {
    private final Logger logger = LogManager.getLogger();
    private final int max = 4096;

    private byte[] compressed = new byte[65536];
    private byte[] encrypted = new byte[65536];
    private final ArrayList<Message> messages = new ArrayList<>(max);

    private final UnsafeByteArrayOutputStream output = new UnsafeByteArrayOutputStream(65536);
    private final DataOutputStream stream = new DataOutputStream(output);
    private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private final Cipher cipher = Cipher.getInstance(ECDHFlow.AES256);
    private final Mac hmac = Mac.getInstance(ECDHFlow.HmacSHA256);

    public ECDHNewEncoder() throws GeneralSecurityException {
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Message) {
            if (msg instanceof ECDHRequest) {
                ctx.write(msg, promise);
            } else if (msg instanceof ECDHResponse) {
                ctx.write(msg, promise);
            } else if (msg instanceof ECDHEncrypted) {
                ctx.write(msg, promise);
            } else {
                assert promise == ctx.voidPromise();
                var message = (Message) msg;
                messages.add(message);
                if (messages.size() >= max) {
                    flush(ctx);
                }
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    private void encrypt(ChannelHandlerContext ctx) throws Exception {
        output.reset();
        stream.writeInt(messages.size());
        for (var message : messages) {
            Codec.Companion.anyCodec().write(stream, message);
        }
        messages.clear();

        var data = output.array();
        var len = output.size();

        // compress
        var maxCompressedLength = compressor.maxCompressedLength(len);
        if (compressed.length < maxCompressedLength) {
            compressed = new byte[maxCompressedLength + 4096];
        }
        var compressedLen = compressor.compress(data, 0, len, compressed, 0);

        // encrypt
        var ecdhFlow = ctx.channel().attr(ECDHFlow.Companion.getKey()).get();
        var secretKey = ecdhFlow.secretKey();
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        var iv = cipher.getIV();
        var encryptedMaxLen = cipher.getOutputSize(compressedLen);
        if (encrypted.length < encryptedMaxLen) {
            encrypted = new byte[encryptedMaxLen + 4096];
        }
        var encryptedLen = cipher.doFinal(compressed, 0, compressedLen, encrypted);

        // sign
        hmac.init(secretKey);
        hmac.update(iv);
        hmac.update(encrypted, 0, encryptedLen);
        var sign = hmac.doFinal();

        // send message
        var msg = new ECDHEncrypted(sign, iv, len, Unpooled.copiedBuffer(encrypted, 0, encryptedLen));
        ctx.write(msg, ctx.voidPromise());
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (!messages.isEmpty()) {
            encrypt(ctx);
        }
        ctx.flush();
    }

    private static class UnsafeByteArrayOutputStream extends ByteArrayOutputStream {
        UnsafeByteArrayOutputStream(int size) {
            super(size);
        }

        byte[] array() {
            return buf;
        }
    }
}
