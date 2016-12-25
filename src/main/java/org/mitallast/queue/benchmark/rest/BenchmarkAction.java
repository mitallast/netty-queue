package org.mitallast.queue.benchmark.rest;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.benchmark.BenchmarkService;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

import java.io.IOException;
import java.math.BigInteger;

public class BenchmarkAction extends BaseRestHandler {

    private final BenchmarkService benchmarkService;

    @Inject
    public BenchmarkAction(Config config, RestController controller, BenchmarkService benchmarkService) {
        super(config, BenchmarkAction.class);
        this.benchmarkService = benchmarkService;

        controller.registerHandler(HttpMethod.GET, "/_benchmark", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        request.content().release();
        int requests = Integer.parseInt(request.param("requests").toString());
        int dataSize = Integer.parseInt(request.param("dataSize").toString());

        benchmarkService.benchmark(requests, dataSize).whenComplete((result, error) -> {
            if (error != null) {
                session.sendResponse(error);
            } else {
                ByteBuf buffer = session.alloc().directBuffer();
                try{
                    try (XStreamBuilder builder = createBuilder(request, buffer)) {
                        builder.writeStartObject();
                        builder.writeNumberField("requests", result.getRequests());
                        builder.writeNumberField("dataSize", result.getDataSize());
                        builder.writeNumberField("start", result.getStart());
                        builder.writeNumberField("end", result.getEnd());
                        builder.writeNumberField("duration", result.getEnd() - result.getStart());

                        BigInteger toSec = BigInteger.valueOf(1000);
                        BigInteger duration = BigInteger.valueOf(result.getEnd() - result.getStart());
                        BigInteger count = BigInteger.valueOf(result.getRequests());

                        BigInteger totalBytes = BigInteger.valueOf(result.getDataSize()).multiply(count);
                        BigInteger bytesPerSec = totalBytes.multiply(toSec).divide(duration);
                        builder.writeNumberField("throughput", bytesPerSec.longValue());

                        BigInteger qps = BigInteger.valueOf(result.getRequests()).multiply(toSec).divide(duration);
                        builder.writeNumberField("qps", qps.longValue());

                        builder.writeEndObject();
                    }
                    session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
                }catch (IOException e){
                    session.sendResponse(e);
                }
            }
        });
    }
}
