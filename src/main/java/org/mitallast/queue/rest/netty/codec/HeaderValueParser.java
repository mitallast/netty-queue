package org.mitallast.queue.rest.netty.codec;

import io.netty.handler.codec.http.HttpConstants;
import io.netty.util.ByteProcessor;

class HeaderValueParser implements ByteProcessor {

    @Override
    public boolean process(byte value) {
        if (value == HttpConstants.CR) {
            return true;
        }
        if (value == HttpConstants.SP) {
            return true;
        }
        if (value == HttpConstants.LF) {
            return false;
        }
        return true;
    }
}
