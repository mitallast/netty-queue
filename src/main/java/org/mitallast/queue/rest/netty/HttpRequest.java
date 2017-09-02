package org.mitallast.queue.rest.netty;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.handler.stream.ChunkedNioFile;
import io.netty.handler.stream.ChunkedStream;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mitallast.queue.common.json.JsonService;
import org.mitallast.queue.rest.ResponseBuilder;
import org.mitallast.queue.rest.RestRequest;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpRequest implements RestRequest {
    private final static Logger logger = LogManager.getLogger();
    private final static MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();

    private static final AsciiString APPLICATION_JAVASCRIPT = new AsciiString("application/javascript");
    private static final AsciiString TEXT_CSS = new AsciiString("text/css");
    private static final AsciiString TEXT_HTML = new AsciiString("text/html");

    private static final DateTimeFormatter dateFormat = DateTimeFormat
        .forPattern("EEE, dd MMM yyyy HH:mm:ss Z")
        .withLocale(Locale.US)
        .withZoneUTC();

    private final ChannelHandlerContext ctx;
    private final FullHttpRequest httpRequest;
    private final HttpMethod httpMethod;
    private final JsonService jsonService;

    private Map<String, String> paramMap;
    private String queryPath;

    public HttpRequest(ChannelHandlerContext ctx, FullHttpRequest request, JsonService jsonService) {
        this.ctx = ctx;
        this.httpRequest = request;
        this.httpMethod = request.method();
        this.jsonService = jsonService;

        parseQueryString();
    }

    @Override
    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

    @Override
    public Map<String, String> getParamMap() {
        return paramMap;
    }

    @Override
    public String param(String param) {
        String val = paramMap.get(param);
        if (val == null) {
            throw new IllegalArgumentException("Param {" + param + "} not found");
        } else {
            return val;
        }
    }

    @Override
    public boolean hasParam(String param) {
        return paramMap.containsKey(param);
    }

    @Override
    public ByteBuf content() {
        return httpRequest.content();
    }

    @Override
    public String getQueryPath() {
        return queryPath;
    }

    @Override
    public String getUri() {
        return httpRequest.uri();
    }

    private void parseQueryString() {
        String uri = httpRequest.uri();

        int pathEndPos = uri.indexOf('?');

        if (pathEndPos < 0) {
            paramMap = new HashMap<>();
            queryPath = uri;
        } else {
            QueryStringDecoder decoder = new QueryStringDecoder(uri);
            queryPath = uri.substring(0, pathEndPos);
            Map<String, List<String>> parameters = decoder.parameters();
            if (parameters.isEmpty()) {
                paramMap = Collections.emptyMap();
            } else {
                paramMap = new HashMap<>(parameters.size());
                parameters.entrySet().stream()
                    .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
                    .forEach(entry -> paramMap.put(entry.getKey(), entry.getValue().get(0)));
            }
        }
    }

    @Override
    public ResponseBuilder response() {
        return new HttpResponseBuilder();
    }

    private class HttpResponseBuilder implements ResponseBuilder {
        private HttpResponseStatus status = HttpResponseStatus.OK;
        private final HttpHeaders headers = new DefaultHttpHeaders(false);

        @Override
        public ResponseBuilder status(int status) {
            this.status = HttpResponseStatus.valueOf(status);
            return this;
        }

        @Override
        public ResponseBuilder status(int status, String reason) {
            Preconditions.checkNotNull(reason);
            this.status = new HttpResponseStatus(status, reason);
            return this;
        }

        @Override
        public ResponseBuilder status(HttpResponseStatus status) {
            Preconditions.checkNotNull(status);
            this.status = status;
            return this;
        }

        @Override
        public ResponseBuilder header(AsciiString name, AsciiString value) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(value);
            headers.add(name, value);
            return this;
        }

        @Override
        public ResponseBuilder header(AsciiString name, String value) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(value);
            headers.add(name, value);
            return this;
        }

        @Override
        public ResponseBuilder header(AsciiString name, long value) {
            Preconditions.checkNotNull(name);
            headers.add(name, value);
            return this;
        }

        @Override
        public void error(Throwable error) {
            Preconditions.checkNotNull(error);
            ByteBuf buffer = ctx.alloc().buffer();
            try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer)) {
                try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(outputStream, CharsetUtil.UTF_8))) {
                    error.printStackTrace(printWriter);
                }
            } catch (IOException e) {
                buffer.release();
                throw new RuntimeException(e);
            }
            header(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
            data(buffer);
        }

        @Override
        public void json(Object json) {
            Preconditions.checkNotNull(json);
            ByteBuf buf = ctx.alloc().buffer();
            jsonService.serialize(buf, json);
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
            data(buf);
        }

        @Override
        public void text(String content) {
            Preconditions.checkNotNull(content);
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
            data(Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));
        }

        @Override
        public void bytes(byte[] content) {
            Preconditions.checkNotNull(content);
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.BINARY);
            data(Unpooled.wrappedBuffer(content));
        }

        @Override
        public void data(ByteBuf content) {
            Preconditions.checkNotNull(content);
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status,
                content,
                headers,
                EmptyHttpHeaders.INSTANCE
            );
            HttpUtil.setContentLength(response, content.readableBytes());
            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true);
            }
            ChannelFuture writeFuture = ctx.writeAndFlush(response);
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void file(URL url) {
            switch (url.getProtocol()) {
                case "file":
                    try {
                        file(new File(url.toURI()));
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case "jar":
                    try {
                        String path = new URI(url.getPath()).getPath();
                        int bangIndex = path.indexOf('!');
                        String filePath = path.substring(0, bangIndex);
                        String resourcePath = path.substring(bangIndex + 2);

                        ZipFile jar = new ZipFile(filePath);
                        ZipEntry entry = jar.getEntry(resourcePath);

                        long lastModified = entry.getTime();
                        if (isModifiedSince(lastModified)) {
                            sendNotModified();
                        } else {
                            InputStream stream = jar.getInputStream(entry);
                            long contentLength = entry.getSize();
                            file(resourcePath, contentLength, lastModified, stream);
                        }
                    } catch (IOException | URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    try {
                        URLConnection conn = url.openConnection();
                        // otherwise the JDK will keep the connection open when we close!
                        conn.setUseCaches(false);

                        long contentLength = conn.getContentLength();
                        long lastModified = conn.getLastModified();
                        InputStream stream = conn.getInputStream();

                        if (isModifiedSince(lastModified)) {
                            stream.close();
                            sendNotModified();
                        } else {
                            file(url.getPath(), contentLength, lastModified, stream);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
            }
        }

        private void file(String path, long contentLength, long lastModified, InputStream stream) {
            logger.info("path: {}", path);

            mimetype(path);

            header(HttpHeaderNames.CONTENT_LENGTH, contentLength);
            lastModified(lastModified);

            DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK, headers);

            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true);
            }
            ctx.write(response);
            ChannelFuture write = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedStream(stream, 8192)));
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                write.addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void file(File file) {
            long lastModified = file.lastModified();
            if (isModifiedSince(lastModified)) {
                sendNotModified();
            } else {
                mimetype(file.getPath());
                lastModified(lastModified);
                header(HttpHeaderNames.CONTENT_LENGTH, file.length());
                DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK, headers);

                if (HttpUtil.isKeepAlive(httpRequest)) {
                    HttpUtil.setKeepAlive(response, true);
                }
                ctx.write(response);

                try {
                    ChannelFuture write = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedNioFile(file)));
                    if (!HttpUtil.isKeepAlive(httpRequest)) {
                        write.addListener(ChannelFutureListener.CLOSE);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void mimetype(String path) {
            if (path.endsWith(".js")) {
                header(HttpHeaderNames.CONTENT_TYPE, APPLICATION_JAVASCRIPT);
            } else if (path.endsWith(".css")) {
                header(HttpHeaderNames.CONTENT_TYPE, TEXT_CSS);
            } else if (path.endsWith(".html")) {
                header(HttpHeaderNames.CONTENT_TYPE, TEXT_HTML);
            } else {
                header(HttpHeaderNames.CONTENT_TYPE, mimeTypesMap.getContentType(path));
            }
        }

        @Override
        public void empty() {
            header(HttpHeaderNames.CONTENT_LENGTH, 0L);
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1,
                status,
                Unpooled.buffer(0),
                headers,
                EmptyHttpHeaders.INSTANCE
            );
            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true);
            }
            ChannelFuture write = ctx.writeAndFlush(response);
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                write.addListener(ChannelFutureListener.CLOSE);
            }
        }

        private void lastModified(long lastModified) {
            header(HttpHeaderNames.LAST_MODIFIED, dateFormat.print(lastModified));
        }

        private boolean isModifiedSince(long lastModified) {
            String ifModifiedSince = httpRequest.headers().get(HttpHeaderNames.IF_MODIFIED_SINCE);
            if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
                try {
                    long ifModifiedSinceSec = dateFormat.parseMillis(ifModifiedSince) / 1000;
                    long lastModifiedSec = lastModified / 1000;
                    return lastModifiedSec == ifModifiedSinceSec;
                } catch (UnsupportedOperationException | IllegalArgumentException e) {
                    logger.warn(e);
                    return false;
                }
            }
            return false;
        }

        private void sendNotModified() {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_MODIFIED);
            response.headers().set(HttpHeaderNames.DATE, dateFormat.print(System.currentTimeMillis()));

            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true);
            }
            ChannelFuture write = ctx.writeAndFlush(response);
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                write.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
