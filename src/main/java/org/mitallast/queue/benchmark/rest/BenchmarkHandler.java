package org.mitallast.queue.benchmark.rest;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.queue.benchmark.BenchmarkService;
import org.mitallast.queue.rest.RestController;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;

public class BenchmarkHandler {

    private final BenchmarkService benchmarkService;

    @Inject
    public BenchmarkHandler(RestController controller, BenchmarkService benchmarkService) {
        this.benchmarkService = benchmarkService;

        controller.handler(this::handleRequest)
            .param(controller.param().toInt("requests"))
            .param(controller.param().toInt("dataSize"))
            .response(controller.response().futureJson())
            .handle(HttpMethod.GET, "_benchmark");
    }

    public CompletableFuture<ImmutableMap<String, Object>> handleRequest(int requests, int dataSize) {
        return benchmarkService.benchmark(requests, dataSize).thenApply(result -> {
            BigInteger toSec = BigInteger.valueOf(1000);
            BigInteger duration = BigInteger.valueOf(result.getEnd() - result.getStart());
            BigInteger count = BigInteger.valueOf(result.getRequests());

            BigInteger totalBytes = BigInteger.valueOf(result.getDataSize()).multiply(count);
            BigInteger bytesPerSec = totalBytes.multiply(toSec).divide(duration);
            BigInteger qps = BigInteger.valueOf(result.getRequests()).multiply(toSec).divide(duration);

            return ImmutableMap.<String, Object>builder()
                .put("requests", result.getRequests())
                .put("dataSize", result.getDataSize())
                .put("start", result.getStart())
                .put("end", result.getEnd())
                .put("duration", result.getEnd() - result.getStart())
                .put("throughput", bytesPerSec.longValue())
                .put("qps", qps.longValue())
                .build();
        });
    }
}
