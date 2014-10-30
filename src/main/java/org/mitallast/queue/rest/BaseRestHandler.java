package org.mitallast.queue.rest;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class BaseRestHandler extends AbstractComponent implements RestHandler {
    protected final Client client;

    private final JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());

    public BaseRestHandler(Settings settings, Client client) {
        super(settings);
        this.client = client;
    }

    protected JsonGenerator createGenerator(RestRequest request, OutputStream outputStream) {
        try {
            JsonGenerator generator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);
            if (request.hasParam("pretty")) {
                generator.setPrettyPrinter(new DefaultPrettyPrinter());
            }
            return generator;
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
    }

    protected JsonParser createParser(InputStream inputStream) throws IOException {
        return jsonFactory.createParser(inputStream);
    }
}
