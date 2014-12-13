package org.mitallast.queue.rest.action.queue.enqueue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.client.QueueClient;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageType;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RestEnQueueActionTest {
    @Mock
    private Settings settings;
    @Mock
    private RestController restController;
    @Mock
    private RestRequest restRequest;
    @Mock
    private RestSession restSession;
    @Mock
    private QueueClient queueClient;
    @Mock
    private Client client;

    @Captor
    private ArgumentCaptor<EnQueueRequest> captor;

    private RestEnQueueAction restEnQueueAction;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(client.queue()).thenReturn(queueClient);
        assert client != null;
        assert client.queue() != null;
        restEnQueueAction = new RestEnQueueAction(settings, client, restController);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testString() throws IOException {

        UUID uuid = UUIDs.generateRandom();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        JsonFactory factory = new JsonFactory();
        JsonGenerator generator = factory.createGenerator(outputStream);
        generator.writeStartObject();
        generator.writeStringField("message", "Hello world");
        generator.writeStringField("uuid", uuid.toString());
        generator.writeEndObject();
        generator.close();

        when(restRequest.param("queue")).thenReturn("testQueue");
        when(restRequest.getInputStream()).thenReturn(new ByteArrayInputStream(outputStream.toByteArray()));

        restEnQueueAction.handleRequest(restRequest, restSession);

        verify(client.queue(), atLeastOnce()).enqueueRequest(captor.capture(), any(ActionListener.class));

        String queue = captor.getValue().getQueue();
        QueueMessage queueMessage = captor.getValue().getMessage();
        assert "testQueue".equals(queue);
        assert "Hello world".equals(queueMessage.getMessage());
        assert uuid.equals(queueMessage.getUuid());
        assert queueMessage.getMessageType() == QueueMessageType.STRING;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testJsonObject() throws IOException {

        UUID uuid = UUIDs.generateRandom();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        JsonFactory factory = new JsonFactory();
        JsonGenerator generator = factory.createGenerator(outputStream);
        generator.writeStartObject();
        generator.writeFieldName("message");
        generator.writeStartObject();
        generator.writeStringField("title", "Hello title");
        generator.writeStringField("description", "Hello description");
        generator.writeEndObject();

        generator.writeStringField("uuid", uuid.toString());
        generator.writeEndObject();
        generator.close();

        when(restRequest.param("queue")).thenReturn("testQueue");
        when(restRequest.getInputStream()).thenReturn(new ByteArrayInputStream(outputStream.toByteArray()));

        restEnQueueAction.handleRequest(restRequest, restSession);

        verify(client.queue(), atLeastOnce()).enqueueRequest(captor.capture(), any(ActionListener.class));

        String queue = captor.getValue().getQueue();
        QueueMessage queueMessage = captor.getValue().getMessage();
        assert "testQueue".equals(queue);
        assert "{\"title\":\"Hello title\",\"description\":\"Hello description\"}".equals(queueMessage.getMessage());
        assert uuid.equals(queueMessage.getUuid());
        assert queueMessage.getMessageType() == QueueMessageType.JSON;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testJsonArray() throws IOException {

        UUID uuid = UUIDs.generateRandom();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        JsonFactory factory = new JsonFactory();
        JsonGenerator generator = factory.createGenerator(outputStream);
        generator.writeStartObject();
        generator.writeFieldName("message");
        generator.writeStartArray();
        generator.writeString("Hello title");
        generator.writeString("Hello description");
        generator.writeEndArray();

        generator.writeStringField("uuid", uuid.toString());
        generator.writeEndObject();
        generator.close();

        when(restRequest.param("queue")).thenReturn("testQueue");
        when(restRequest.getInputStream()).thenReturn(new ByteArrayInputStream(outputStream.toByteArray()));

        restEnQueueAction.handleRequest(restRequest, restSession);

        verify(client.queue(), atLeastOnce()).enqueueRequest(captor.capture(), any(ActionListener.class));

        String queue = captor.getValue().getQueue();
        QueueMessage queueMessage = captor.getValue().getMessage();
        assert "testQueue".equals(queue);
        assert "[\"Hello title\",\"Hello description\"]".equals(queueMessage.getMessage());
        assert uuid.equals(queueMessage.getUuid());
        assert queueMessage.getMessageType() == QueueMessageType.JSON;
    }
}
