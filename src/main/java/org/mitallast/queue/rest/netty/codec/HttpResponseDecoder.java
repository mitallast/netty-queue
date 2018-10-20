package org.mitallast.queue.rest.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class HttpResponseDecoder extends ByteToMessageDecoder {

    private enum State {INIT, VERSION, STATUS, HEADER_NAME, HEADER_VALUE, HEADER, BODY, BUILD}

    private State state = State.INIT;

    private HttpVersion version = null;
    private HttpResponseStatus status = null;
    private int contentLength = 0;
    private HttpHeaderName headerName = null;
    private AsciiString headerValue = null;
    private HttpHeaders headers;
    private ByteBuf content;

    private final VersionParser versionParser = new VersionParser();
    private final StatusParser statusParser = new StatusParser();

    private final HeaderNameParser headerNameParser = new HeaderNameParser();
    private final HeaderValueParser headerValueParser = new HeaderValueParser();

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.isReadable()) {
            if (state == State.INIT) {
                version = null;
                status = null;
                contentLength = 0;
                headerName = null;
                headerValue = null;
                headers = HttpHeaders.newInstance();
                content = null;
                state = State.VERSION;
            }
            if (state == State.VERSION) {
                if (!readVersion(in)) {
                    return;
                }
            }
            if (state == State.STATUS) {
                if (!readStatus(in)) {
                    return;
                }
            }
            if (state == State.HEADER_NAME || state == State.HEADER_VALUE || state == State.HEADER) {
                do {
                    if (state == State.HEADER_NAME) {
                        if (!readHeaderName(in)) {
                            return;
                        }
                    }
                    if (state == State.HEADER_VALUE) {
                        if (!readHeaderValue(in)) {
                            return;
                        }
                    }
                    if (state == State.HEADER) {
                        parseHeader();
                    }
                } while (state != State.BODY);
            }
            if (state == State.BODY) {
                if (!readBody(in)) {
                    return;
                }
            }
            if (state == State.BUILD) {
                HttpResponse request = new HttpResponse(version, status, headers, content);
                out.add(request);
                state = State.INIT;
            }
        }
    }

    private boolean readVersion(ByteBuf in) {
        versionParser.reset();
        int i = in.forEachByte(versionParser);
        if (i > 0) {
            in.readerIndex(i + 1);
            version = versionParser.version();
            state = State.STATUS;
            return true;
        }
        return false;
    }

    private boolean readStatus(ByteBuf in) {
        statusParser.reset();
        int i = in.forEachByte(statusParser);
        if (i > 0) {
            in.readerIndex(i + 1);
            status = statusParser.status();
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
                return false;
            } else {
                headerName = headerNameParser.find();
                state = State.HEADER_VALUE;
                return true;
            }
        }
    }

    private boolean readHeaderValue(ByteBuf in) {
        final int i = in.forEachByte(headerValueParser);
        if (i > 0) {
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
            state = State.BUILD;
            return true;
        }
    }

}
