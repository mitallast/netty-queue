package org.mitallast.queue.rest.netty.codec;

import io.netty.handler.codec.http.HttpConstants;
import io.netty.util.ByteProcessor;

class UriParser implements ByteProcessor {
    private int pos = 0;

    public void reset() {
        pos = 0;
    }

    @Override
    public boolean process(byte value) {
        if (value == HttpConstants.SP && pos == 0) {
            return true;
        }
        if (value == HttpConstants.SP) {
            return false;
        }
        pos++;
        return true;
    }
}
