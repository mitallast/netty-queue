package org.mitallast.queue.rest;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.IOException;
import java.io.OutputStream;

public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {
    protected final Client client;

    public BaseRestHandler(Settings settings, Client client) {
        super(settings);
        this.client = client;
    }

    protected JsonGenerator getGenerator(RestRequest request, OutputStream outputStream) {
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator generator = factory.createGenerator(outputStream);
            if (request.hasParam("pretty")) {
                generator.setPrettyPrinter(new DefaultPrettyPrinter());
            }
            return generator;
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
    }
}
