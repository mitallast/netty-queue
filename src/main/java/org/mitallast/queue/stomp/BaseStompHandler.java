package org.mitallast.queue.stomp;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.stomp.StompFrame;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class BaseStompHandler extends AbstractComponent implements StompHandler {
    protected final Client client;

    private final JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());

    public BaseStompHandler(Settings settings, Client client) {
        super(settings);
        this.client = client;
    }

    protected JsonGenerator createGenerator(StompFrame request, OutputStream outputStream) {
        try {
            JsonGenerator generator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);
            if (request.headers().contains("pretty")) {
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
