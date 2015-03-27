package org.mitallast.queue.common.xstream;

import org.mitallast.queue.common.xstream.json.JsonXStream;

public class XStreamFactory {

    public static XStream jsonStream() {
        return JsonXStream.jsonXContent;
    }
}
