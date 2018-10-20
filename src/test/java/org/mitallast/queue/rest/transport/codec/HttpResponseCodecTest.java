package org.mitallast.queue.rest.transport.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.mitallast.queue.rest.netty.codec.*;

import java.nio.charset.Charset;
import java.util.ArrayList;

public class HttpResponseCodecTest {

    @Test
    public void testCodec() throws Exception {
        HttpResponse expected = new HttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK,
            HttpHeaders.newInstance(),
            Unpooled.copiedBuffer("hello world", Charset.forName("ASCII"))
        );
        expected.headers().put(HttpHeaderName.CONTENT_LENGTH, AsciiString.of("11"));
        expected.headers().put(HttpHeaderName.CONTENT_TYPE, AsciiString.of("text"));

        HttpResponseEncoder encoder = new HttpResponseEncoder();
        HttpResponseDecoder decoder = new HttpResponseDecoder();

        ByteBuf out = Unpooled.buffer();
        encoder.encode(null, expected, out);

        ArrayList<Object> parsed = new ArrayList<>(1);
        decoder.decode(null, out, parsed);
        assert out.readableBytes() == 0;

        assert parsed.size() == 1;
        HttpResponse actual = (HttpResponse) parsed.get(0);

        assert actual.version() == HttpVersion.HTTP_1_1;
        assert actual.status() == HttpResponseStatus.OK;
        assert actual.headers().containsKey(HttpHeaderName.CONTENT_LENGTH);
        assert actual.headers().get(HttpHeaderName.CONTENT_LENGTH).toString().equals("11");
        assert actual.headers().containsKey(HttpHeaderName.CONTENT_TYPE);
        assert actual.headers().get(HttpHeaderName.CONTENT_TYPE).toString().equals("text");
        assert actual.content() != null;
        assert actual.content().readableBytes() == 11;
        assert actual.content().toString(Charset.forName("ASCII")).equals("hello world");
    }

    @Test
    public void benchmarkEncode() throws Exception {
        HttpResponse response = new HttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK,
            HttpHeaders.newInstance(),
            Unpooled.copiedBuffer("hello world", Charset.forName("ASCII"))
        ) {
            @Override
            public void release() {
                // do nothing for test
            }
        };
        response.headers().put(HttpHeaderName.CONTENT_LENGTH, AsciiString.of("11"));
        response.headers().put(HttpHeaderName.CONTENT_TYPE, AsciiString.of("text"));

        HttpResponseEncoder encoder = new HttpResponseEncoder();

        while (!Thread.interrupted()) {
            int max = 5000000;
            ByteBuf out = Unpooled.buffer();
            long start = System.currentTimeMillis();
            for (int i = 0; i < max; i++) {
                out.writerIndex(0);
                encoder.encode(null, response, out);
            }
            long end = System.currentTimeMillis();
            System.out.println("encode done at " + (end - start) + "ms");
            System.out.println("m/s " + (max * 1000.0 / (end - start)));
        }
    }

    @Test
    public void benchmarkDecode() throws Exception {
        String request =
            "HTTP/1.1 200 OK\r\n" +
                "content-type: plain/text\r\n" +
                "content-length: 11\r\n" +
                "\r\n" +
                "hello world";

        ByteBuf message = Unpooled.copiedBuffer(request, Charset.forName("ASCII"));

        HttpResponseDecoder decoder = new HttpResponseDecoder();
        int max = 5000000;
        ArrayList<Object> parsed = new ArrayList<>(1);

        while (!Thread.interrupted()) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < max; i++) {
                message.readerIndex(0);
                parsed.clear();
                decoder.decode(null, message, parsed);
                ((HttpResponse) parsed.get(0)).release();
            }
            long end = System.currentTimeMillis();
            System.out.println("decode done at " + (end - start) + "ms");
            System.out.println("m/s " + (max * 1000.0 / (end - start)));
        }
    }
}
