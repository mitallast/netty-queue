package org.mitallast.queue.common.xstream;

import org.mitallast.queue.common.xstream.json.JsonXStream;

import java.io.IOException;

public class XStreamFactory {

    public static XStream jsonStream() throws IOException {
        return JsonXStream.jsonXContent;
    }
}
