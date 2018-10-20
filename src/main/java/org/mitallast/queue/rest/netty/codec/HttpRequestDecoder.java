package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class HttpRequestDecoder extends ByteToMessageDecoder {
    private final Logger logger = LogManager.getLogger();

    private enum State {INIT, METHOD, URI, VERSION, HEADER_NAME, HEADER_VALUE, HEADER, BODY, BUILD}

    private State state = State.INIT;

    private HttpMethod method = null;
    private AsciiString uri = null;
    private HttpVersion version = null;
    private int contentLength = 0;
    private HttpHeaderName headerName = null;
    private AsciiString headerValue = null;
    private HttpHeaders headers;
    private ByteBuf content;

    private final MethodParser methodParser = new MethodParser();
    private final UriParser uriParser = new UriParser();
    private final VersionParser versionParser = new VersionParser();
    private final HeaderNameParser headerNameParser = new HeaderNameParser();
    private final HeaderValueParser headerValueParser = new HeaderValueParser();

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.isReadable()) {
            if (state == State.INIT) {
                method = null;
                uri = null;
                version = null;
                contentLength = 0;
                headerName = null;
                headerValue = null;
                headers = HttpHeaders.newInstance();
                content = null;
                state = State.METHOD;
            }
            if (state == State.METHOD) {
                if (!readMethod(in)) {
                    return;
                }
            }
            if (state == State.URI) {
                if (!readUri(in)) {
                    return;
                }
            }
            if (state == State.VERSION) {
                if (!readVersion(in)) {
                    return;
                }
            }
            if (state == State.HEADER_NAME || state == State.HEADER_VALUE || state == State.HEADER) {
                do {
                    if (state == State.HEADER_NAME) {
                        //logger.info("parse header name");
                        if (!readHeaderName(in)) {
                            return;
                        }
                    }
                    if (state == State.HEADER_VALUE) {
                        //logger.info("parse header value");
                        if (!readHeaderValue(in)) {
                            return;
                        }
                    }
                    if (state == State.HEADER) {
                        //logger.info("parse header");
                        parseHeader();
                        assert state != State.HEADER;
                    }
                } while (state != State.BODY);
            }
            if (state == State.BODY) {
                //logger.info("parse body");
                if (!readBody(in)) {
                    return;
                }
            }
            if (state == State.BUILD) {
                //logger.info("build");
                HttpRequest request = new HttpRequest(method, version, uri, headers, content);
                out.add(request);
                state = State.INIT;
            }
        }
    }

    private boolean readMethod(ByteBuf in) {
        methodParser.reset();
        int i = in.forEachByte(methodParser);
        if (i > 0) {
            in.readerIndex(i + 1);
            method = methodParser.find();
            assert method != null;
            state = State.URI;
            return true;
        }
        return false;
    }

    private boolean readUri(ByteBuf in) {
        uriParser.reset();
        int i = in.forEachByte(uriParser);
        if (i > 0) {
            int start = in.readerIndex();
            while (in.getByte(start) == ' ') {
                start++;
            }
            ByteBuf slice = in.slice(start, i - start);
            slice.retain();
            in.readerIndex(i + 1);
            uri = AsciiString.of(slice);
            state = State.VERSION;
            return true;
        } else {
            return false;
        }
    }

    private boolean readVersion(ByteBuf in) {
        versionParser.reset();
        int i = in.forEachByte(versionParser);
        if (i > 0) {
            in.readerIndex(i + 1);
            version = versionParser.version();
            state = State.HEADER_NAME;
            return true;
        }
        return false;
    }

    private boolean readHeaderName(ByteBuf in) {
        headerNameParser.reset();
        int i = in.forEachByte(headerNameParser);
        if (i == -1) {
            return false;
        } else {
            in.readerIndex(i + 1);
            if (headerNameParser.endOfHeaders()) {
                state = State.BODY;
                return true;
            } else {
                headerName = headerNameParser.find();
                assert headerName != null;
                state = State.HEADER_VALUE;
                return true;
            }
        }
    }

    private boolean readHeaderValue(ByteBuf in) {
        final int i = in.forEachByte(headerValueParser);
        if (i > 0 && headerName == null) {
            headerValue = null;
            in.readerIndex(i + 1);
            state = State.HEADER;
            return true;
        } else if (i > 0) {
            int start = in.readerIndex();
            while (in.getByte(start) == ' ') {
                start++;
            }
            int end = i;
            while (end > start) {
                byte c = in.getByte(end);
                if (c == '\r' || c == '\n' || c == ' ') {
                    end--;
                } else {
                    break;
                }
            }
            ByteBuf slice = in.slice(start, end - start + 1);
            slice.retain();
            in.readerIndex(i + 1);
            headerValue = AsciiString.of(slice);
            state = State.HEADER;
            return true;
        } else {
            return false;
        }
    }

    private void parseHeader() {
        if (headerName != null && headerValue != null) {
            headers.put(headerName, headerValue);
            switch (headerName) {
                case CONTENT_LENGTH:
                    contentLength = headerValue.intValue();
                    break;
            }
        }
        state = State.HEADER_NAME;
    }

    private boolean readBody(ByteBuf in) {
        if (contentLength > 0) {
            if (in.readableBytes() < contentLength) {
                return false;
            }
            this.content = in.readSlice(contentLength).retain();
            state = State.BUILD;
            return true;
        } else {
            this.content = Unpooled.EMPTY_BUFFER;
            state = State.BUILD;
            return true;
        }
    }

}
