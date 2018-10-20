package org.mitallast.queue.rest.netty.codec;

import io.netty.handler.codec.http.HttpConstants;
import io.netty.util.ByteProcessor;

class MethodParser implements ByteProcessor {
    private final byte[] buffer = new byte[8];
    private int pos = 0;

    public void reset() {
        pos = 0;
    }

    @Override
    public boolean process(byte value) {
        if (value == HttpConstants.SP) {
            return false;
        }

        buffer[pos] = AsciiString.toLowerCase(value);
        pos++;
        return true;
    }

    public HttpMethod find() {
        switch (buffer[0]) {
            case 'o':
                assert buffer[1] == 'p';
                assert buffer[2] == 't';
                assert buffer[3] == 'i';
                assert buffer[4] == 'o';
                assert buffer[5] == 'n';
                assert buffer[6] == 's';
                return HttpMethod.OPTIONS;
            case 'g':
                assert buffer[1] == 'e';
                assert buffer[2] == 't';
                return HttpMethod.GET;
            case 'h':
                assert buffer[1] == 'e';
                assert buffer[2] == 'a';
                assert buffer[3] == 'd';
                return HttpMethod.HEAD;
            case 'p':
                switch (buffer[1]) {
                    case 'o':
                        assert buffer[2] == 's';
                        assert buffer[3] == 't';
                        return HttpMethod.POST;
                    case 'u':
                        assert buffer[2] == 't';
                        return HttpMethod.PUT;
                    case 'a':
                        assert buffer[2] == 't';
                        assert buffer[3] == 'c';
                        assert buffer[4] == 'h';
                        return HttpMethod.PATCH;
                    default:
                        throw new IllegalStateException("unexpected chars, http method expected");
                }
            case 'd':
                assert buffer[1] == 'e';
                assert buffer[2] == 'l';
                assert buffer[3] == 'e';
                assert buffer[4] == 't';
                assert buffer[5] == 'e';
                return HttpMethod.DELETE;
            case 't':
                assert buffer[1] == 'r';
                assert buffer[2] == 'a';
                assert buffer[3] == 'c';
                assert buffer[4] == 'e';
                return HttpMethod.TRACE;
            case 'c':
                assert buffer[1] == 'o';
                assert buffer[2] == 'n';
                assert buffer[3] == 'n';
                assert buffer[4] == 'e';
                assert buffer[5] == 'c';
                assert buffer[6] == 't';
                return HttpMethod.CONNECT;
            default:
                throw new IllegalStateException("unexpected chars, http method expected");
        }
    }
}
