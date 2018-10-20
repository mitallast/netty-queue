package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.openjdk.jmh.annotations.*;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 3)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class HttpHeaderNameParserBenchmark {

    private final HeaderNameParser headerNameParser = new HeaderNameParser();

    public HttpHeaderNameParserBenchmark() {
        ByteBuf byteBuf = Unpooled.copiedBuffer("content-length: ", Charset.forName("UTF-8"));
        byteBuf.forEachByte(headerNameParser);
    }

    @Benchmark
    public Object find() {
        return headerNameParser.find();
    }
}
