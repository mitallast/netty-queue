package org.mitallast.queue.common.xstream;

import java.io.IOException;

public interface ToXStream {

    void toXStream(XStreamBuilder builder) throws IOException;
}
