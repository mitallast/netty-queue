package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.Map;

import static io.netty.handler.codec.http.HttpConstants.*;

public class HttpRequestEncoder extends MessageToByteEncoder<HttpRequest> {
    private static final int CRLF_SHORT = (CR << 8) | LF;
    private static final int COLONSP_SHORT = (COLON << 8) | SP;

    @Override
    public void encode(ChannelHandlerContext ctx, HttpRequest request, ByteBuf out) {
        request.method().ascii().write(out);
        out.writeByte(' ');
        request.uri().write(out);
        out.writeByte(' ');
        request.version().ascii().write(out);
        out.writeShort(CRLF_SHORT);

        Map<HttpHeaderName, AsciiString> headers = request.headers();
        if (headers != null) {
            for (Map.Entry<HttpHeaderName, AsciiString> header : headers.entrySet()) {
                header.getKey().ascii().write(out);
                out.writeShort(COLONSP_SHORT);
                header.getValue().write(out);
                out.writeShort(CRLF_SHORT);
            }
        }

        out.writeShort(CRLF_SHORT);

        ByteBuf body = request.body();
        if (body != null && body.readableBytes() > 0) {
            body.markReaderIndex();
            out.writeBytes(body);
            body.resetReaderIndex();
        }

        request.release();
    }
}
