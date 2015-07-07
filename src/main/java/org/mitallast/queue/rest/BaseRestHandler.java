package org.mitallast.queue.rest;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.common.xstream.XStreamFactory;
import org.mitallast.queue.common.xstream.XStreamParser;

import java.io.IOException;
import java.io.InputStream;

public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {

    public BaseRestHandler(Settings settings) {
        super(settings);
    }

    protected XStreamParser createParser(InputStream inputStream) throws IOException {
        return XStreamFactory.jsonStream().createParser(inputStream);
    }

    protected XStreamParser createParser(ByteBuf buffer) throws IOException {
        return XStreamFactory.jsonStream().createParser(buffer);
    }

    protected XStreamBuilder createBuilder(RestRequest request, ByteBuf buffer) throws IOException {
        XStreamBuilder generator = XStreamFactory.jsonStream().createGenerator(buffer);
        if (request.hasParam("pretty")) {
            generator.usePrettyPrint();
        }
        return generator;
    }
}
