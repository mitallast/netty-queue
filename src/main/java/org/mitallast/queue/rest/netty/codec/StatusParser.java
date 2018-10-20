package org.mitallast.queue.rest.netty.codec;

import io.netty.handler.codec.http.HttpConstants;
import io.netty.util.ByteProcessor;

class StatusParser implements ByteProcessor {
    private final byte[] buffer = new byte[3];
    private int pos = 0;
    private boolean skip = false;

    public void reset() {
        pos = 0;
        skip = false;
    }

    @Override
    public boolean process(byte value) {
        if (value == HttpConstants.CR) {
            return true;
        }
        if (value == HttpConstants.LF) {
            return false;
        }

        if (!skip) {
            if (value == HttpConstants.SP && pos == 0) {
                return true;
            }
            if (value == HttpConstants.SP) {
                skip = true;
                return true;
            }

            if (!(value >= '0' && value <= '9')) {
                throw new IllegalArgumentException("expected digit, got: " + value);
            }

            if (pos > 2) {
                throw new IllegalStateException("http status code too long");
            }
            buffer[pos] = value;
            pos++;
            return true;
        } else {
            return true;
        }
    }

    public int code() {
        return (buffer[0] - '0') * 100 +
            (buffer[1] - '0') * 10 +
            (buffer[2] - '0');
    }

    public HttpResponseStatus status() {
        return HttpResponseStatus.byCode(code());
    }
}
