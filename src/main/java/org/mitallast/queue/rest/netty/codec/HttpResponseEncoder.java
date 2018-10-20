package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.Map;

import static io.netty.handler.codec.http.HttpConstants.*;

public class HttpResponseEncoder extends MessageToByteEncoder<HttpResponse> {
    private static final int CRLF_SHORT = (CR << 8) | LF;
    private static final int COLONSP_SHORT = (COLON << 8) | SP;

    @Override
    public void encode(ChannelHandlerContext ctx, HttpResponse response, ByteBuf out) {
        response.version().ascii().write(out);
        out.writeByte(' ');
        response.status().codeAsAscii().write(out);
        out.writeByte(' ');
        response.status().message().write(out);
        out.writeShort(CRLF_SHORT);

        Map<HttpHeaderName, AsciiString> headers = response.headers();
        if (headers != null) {
            for (Map.Entry<HttpHeaderName, AsciiString> header : headers.entrySet()) {
                header.getKey().ascii().write(out);
                out.writeShort(COLONSP_SHORT);
                header.getValue().write(out);
                out.writeShort(CRLF_SHORT);
            }
        }
        out.writeShort(CRLF_SHORT);

        ByteBuf body = response.content();
        if (body != null && body.readableBytes() > 0) {
            body.markReaderIndex();
            out.writeBytes(body);
            body.resetReaderIndex();
        }

        response.release();
    }
}
