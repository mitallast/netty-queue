package org.mitallast.queue.rest.netty.codec;

import io.netty.handler.codec.http.HttpConstants;
import io.netty.util.ByteProcessor;

class VersionParser implements ByteProcessor {
    private final byte[] buffer = new byte[8];
    private int pos = 0;

    public void reset() {
        pos = 0;
    }

    @Override
    public boolean process(byte value) {
        if (value == HttpConstants.CR) {
            return true;
        }
        if (value == HttpConstants.SP && pos == 0) {
            return true;
        }
        if (value == HttpConstants.LF || value == HttpConstants.SP) {
            return false;
        }

        buffer[pos] = AsciiString.toLowerCase(value);
        pos++;
        return true;
    }

    public HttpVersion version() {
        assert buffer[0] == 'h';
        assert buffer[1] == 't';
        assert buffer[2] == 't';
        assert buffer[3] == 'p';
        assert buffer[4] == '/';
        assert buffer[5] == '1';
        assert buffer[6] == '.';
        switch (buffer[7]) {
            case '0':
                return HttpVersion.HTTP_1_0;
            case '1':
                return HttpVersion.HTTP_1_1;
            default:
                throw new IllegalStateException("unexpected chars, http version expected");
        }
    }
}
