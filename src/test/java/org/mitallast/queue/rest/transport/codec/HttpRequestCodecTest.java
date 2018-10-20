package org.mitallast.queue.rest.transport.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.mitallast.queue.rest.netty.codec.*;

import java.nio.charset.Charset;
import java.util.ArrayList;

public class HttpRequestCodecTest {

    @Test
    public void requestCodec() throws Exception {
        HttpRequest expected = new HttpRequest(
            HttpMethod.POST,
            HttpVersion.HTTP_1_1,
            AsciiString.of("/test/path"),
            HttpHeaders.newInstance(),
            Unpooled.copiedBuffer("hello world", Charset.forName("ASCII"))
        );
        expected.headers().put(HttpHeaderName.CONTENT_LENGTH, AsciiString.of("11"));
        expected.headers().put(HttpHeaderName.CONTENT_TYPE, AsciiString.of("text"));
        expected.headers().put(HttpHeaderName.ACCEPT, AsciiString.of("*/*"));

        HttpRequestEncoder encoder = new HttpRequestEncoder();
        HttpRequestDecoder decoder = new HttpRequestDecoder();

        ByteBuf out = Unpooled.buffer();
        encoder.encode(null, expected, out);

        ArrayList<Object> parsed = new ArrayList<>(1);
        decoder.decode(null, out, parsed);
        assert out.readableBytes() == 0;

        assert parsed.size() == 1;
        HttpRequest actual = (HttpRequest) parsed.get(0);

        assert actual.method() == HttpMethod.POST;
        assert actual.version() == HttpVersion.HTTP_1_1;
        assert actual.uri().toString().equals("/test/path");
        assert actual.headers().containsKey(HttpHeaderName.CONTENT_LENGTH);
        assert actual.headers().get(HttpHeaderName.CONTENT_LENGTH).toString().equals("11");
        assert actual.headers().containsKey(HttpHeaderName.CONTENT_TYPE);
        assert actual.headers().get(HttpHeaderName.CONTENT_TYPE).toString().equals("text");
        assert actual.headers().containsKey(HttpHeaderName.ACCEPT);
        assert actual.headers().get(HttpHeaderName.ACCEPT).toString().equals("*/*");
        assert actual.body() != null;
        assert actual.body().readableBytes() == 11;
        assert actual.body().toString(Charset.forName("ASCII")).equals("hello world");
    }

    @Test
    public void benchmarkEncode() throws Exception {
        HttpRequest request = new HttpRequest(
            HttpMethod.POST,
            HttpVersion.HTTP_1_1,
            AsciiString.of("/test/path"),
            HttpHeaders.newInstance(),
            Unpooled.copiedBuffer("hello world", Charset.forName("ASCII"))
        ) {
            @Override
            public void release() {
                // do nothing for test
            }
        };
        request.headers().put(HttpHeaderName.CONTENT_LENGTH, AsciiString.of("11"));
        request.headers().put(HttpHeaderName.CONTENT_TYPE, AsciiString.of("text"));

        HttpRequestEncoder encoder = new HttpRequestEncoder();

        while (!Thread.interrupted()) {
            int max = 5000000;
            ByteBuf out = Unpooled.buffer();
            long start = System.currentTimeMillis();
            for (int i = 0; i < max; i++) {
                out.writerIndex(0);
                encoder.encode(null, request, out);
            }
            long end = System.currentTimeMillis();
            System.out.println("encode done at " + (end - start) + "ms");
            System.out.println("m/s " + (max * 1000.0 / (end - start)));
        }
    }

    @Test
    public void benchmarkDecode() throws Exception {
        String request =
            "POST /test/path HTTP/1.1\r\n" +
                "content-type: plain/text\r\n" +
                "content-length: 11\r\n" +
                "\r\n" +
                "hello world";


        ByteBuf message = Unpooled.copiedBuffer(request, Charset.forName("ASCII"));

        HttpRequestDecoder decoder = new HttpRequestDecoder();
        int max = 5000000;
        ArrayList<Object> parsed = new ArrayList<>(1);

        while (!Thread.interrupted()) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < max; i++) {
                message.readerIndex(0);
                parsed.clear();
                decoder.decode(null, message, parsed);
                ((HttpRequest) parsed.get(0)).release();
            }
            long end = System.currentTimeMillis();
            System.out.println("decode done at " + (end - start) + "ms");
            System.out.println("m/s " + (max * 1000.0 / (end - start)));
        }
    }
}
