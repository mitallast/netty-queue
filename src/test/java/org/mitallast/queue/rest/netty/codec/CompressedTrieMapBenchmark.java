package org.mitallast.queue.rest.netty.codec;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.AsciiString;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.Control;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.HashMap;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
public class CompressedTrieMapBenchmark {

    private final ArrayList<AsciiString> headers;
    private final CompressedTrieMap<AsciiString> trieMap;
    private final HashMap<String, AsciiString> hashMap;
    private final StringBuilder stringBuilder = new StringBuilder();

    private int i = 0;

    public CompressedTrieMapBenchmark() {
        headers = new ArrayList<>();
        headers.add(HttpHeaderNames.ACCEPT);
        headers.add(HttpHeaderNames.ACCEPT_CHARSET);
        headers.add(HttpHeaderNames.ACCEPT_ENCODING);
        headers.add(HttpHeaderNames.ACCEPT_LANGUAGE);
        headers.add(HttpHeaderNames.ACCEPT_RANGES);
        headers.add(HttpHeaderNames.ACCEPT_PATCH);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_REQUEST_HEADERS);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD);
        headers.add(HttpHeaderNames.AGE);
        headers.add(HttpHeaderNames.ALLOW);
        headers.add(HttpHeaderNames.AUTHORIZATION);
        headers.add(HttpHeaderNames.CACHE_CONTROL);
        headers.add(HttpHeaderNames.CONNECTION);
        headers.add(HttpHeaderNames.CONTENT_BASE);
        headers.add(HttpHeaderNames.CONTENT_ENCODING);
        headers.add(HttpHeaderNames.CONTENT_LANGUAGE);
        headers.add(HttpHeaderNames.CONTENT_LENGTH);
        headers.add(HttpHeaderNames.CONTENT_LOCATION);
        headers.add(HttpHeaderNames.CONTENT_TRANSFER_ENCODING);
        headers.add(HttpHeaderNames.CONTENT_DISPOSITION);
        headers.add(HttpHeaderNames.CONTENT_MD5);
        headers.add(HttpHeaderNames.CONTENT_RANGE);
        headers.add(HttpHeaderNames.CONTENT_SECURITY_POLICY);
        headers.add(HttpHeaderNames.CONTENT_TYPE);
        headers.add(HttpHeaderNames.COOKIE);
        headers.add(HttpHeaderNames.DATE);
        headers.add(HttpHeaderNames.ETAG);
        headers.add(HttpHeaderNames.EXPECT);
        headers.add(HttpHeaderNames.EXPIRES);
        headers.add(HttpHeaderNames.FROM);
        headers.add(HttpHeaderNames.HOST);
        headers.add(HttpHeaderNames.IF_MATCH);
        headers.add(HttpHeaderNames.IF_MODIFIED_SINCE);
        headers.add(HttpHeaderNames.IF_NONE_MATCH);
        headers.add(HttpHeaderNames.IF_RANGE);
        headers.add(HttpHeaderNames.IF_UNMODIFIED_SINCE);
        headers.add(HttpHeaderNames.KEEP_ALIVE);
        headers.add(HttpHeaderNames.LAST_MODIFIED);
        headers.add(HttpHeaderNames.LOCATION);
        headers.add(HttpHeaderNames.MAX_FORWARDS);
        headers.add(HttpHeaderNames.ORIGIN);
        headers.add(HttpHeaderNames.PRAGMA);
        headers.add(HttpHeaderNames.PROXY_AUTHENTICATE);
        headers.add(HttpHeaderNames.PROXY_AUTHORIZATION);
        headers.add(HttpHeaderNames.PROXY_CONNECTION);
        headers.add(HttpHeaderNames.RANGE);
        headers.add(HttpHeaderNames.REFERER);
        headers.add(HttpHeaderNames.RETRY_AFTER);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_KEY1);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_KEY2);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_LOCATION);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_ORIGIN);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_VERSION);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_KEY);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_ACCEPT);
        headers.add(HttpHeaderNames.SEC_WEBSOCKET_EXTENSIONS);
        headers.add(HttpHeaderNames.SERVER);
        headers.add(HttpHeaderNames.SET_COOKIE);
        headers.add(HttpHeaderNames.SET_COOKIE2);
        headers.add(HttpHeaderNames.TE);
        headers.add(HttpHeaderNames.TRAILER);
        headers.add(HttpHeaderNames.TRANSFER_ENCODING);
        headers.add(HttpHeaderNames.UPGRADE);
        headers.add(HttpHeaderNames.USER_AGENT);
        headers.add(HttpHeaderNames.VARY);
        headers.add(HttpHeaderNames.VIA);
        headers.add(HttpHeaderNames.WARNING);
        headers.add(HttpHeaderNames.WEBSOCKET_LOCATION);
        headers.add(HttpHeaderNames.WEBSOCKET_ORIGIN);
        headers.add(HttpHeaderNames.WEBSOCKET_PROTOCOL);
        headers.add(HttpHeaderNames.WWW_AUTHENTICATE);
        headers.add(HttpHeaderNames.X_FRAME_OPTIONS);

        hashMap = new HashMap<>();
        CompressedTrieMap.Builder<AsciiString> builder = CompressedTrieMap.builder();
        for (AsciiString header : headers) {
            builder.add(header.toString(), header);
            hashMap.put(header.toString(), header);
        }
        trieMap = builder.build();
        trieMap.print();
    }

    @Benchmark
    public void trieMap(Blackhole blackhole) {
        i = (i + 1) % headers.size();
        stringBuilder.setLength(0);
        stringBuilder.append(headers.get(i).toString());
        String key = stringBuilder.toString();
        blackhole.consume(key);
    }

    @Benchmark
    public void hashMap(Blackhole blackhole) {
        i = (i + 1) % headers.size();
        stringBuilder.setLength(0);
        stringBuilder.append(headers.get(i).toString());
        String key = stringBuilder.toString();
        blackhole.consume(hashMap.get(key));
    }
}
