package org.mitallast.queue.rest.action.queue.enqueue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.client.QueueClient;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageType;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RestEnQueueActionTest extends BaseTest {
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

        UUID uuid = randomUUID();

        ByteBuf buffer = Unpooled.buffer();
        try (XStreamBuilder builder = jsonBuilder(buffer)) {
            builder.writeStartObject();
            builder.writeStringField("message", "Hello world");
            builder.writeStringField("uuid", uuid.toString());
            builder.writeEndObject();
        }

        when(restRequest.param("queue")).thenReturn("testQueue");
        when(restRequest.content()).thenReturn(buffer);

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

        UUID uuid = randomUUID();

        ByteBuf buffer = Unpooled.buffer();
        try (XStreamBuilder builder = jsonBuilder(buffer)) {
            builder.writeStartObject();
            builder.writeFieldName("message");
            builder.writeStartObject();
            builder.writeStringField("title", "Hello title");
            builder.writeStringField("description", "Hello description");
            builder.writeEndObject();

            builder.writeStringField("uuid", uuid.toString());
            builder.writeEndObject();
        }

        when(restRequest.param("queue")).thenReturn("testQueue");
        when(restRequest.content()).thenReturn(buffer);

        restEnQueueAction.handleRequest(restRequest, restSession);

        verify(client.queue(), atLeastOnce()).enqueueRequest(captor.capture(), any(ActionListener.class));

        String queue = captor.getValue().getQueue();
        QueueMessage queueMessage = captor.getValue().getMessage();
        assert "testQueue".equals(queue);
        Assert.assertEquals("{\"title\":\"Hello title\",\"description\":\"Hello description\"}", queueMessage.getMessage());
        assert uuid.equals(queueMessage.getUuid());
        assert queueMessage.getMessageType() == QueueMessageType.JSON;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testJsonArray() throws IOException {

        UUID uuid = randomUUID();

        ByteBuf buffer = Unpooled.buffer();
        try (XStreamBuilder builder = jsonBuilder(buffer)) {
            builder.writeStartObject();
            builder.writeFieldName("message");
            builder.writeStartArray();
            builder.writeString("Hello title");
            builder.writeString("Hello description");
            builder.writeEndArray();

            builder.writeStringField("uuid", uuid.toString());
            builder.writeEndObject();
        }

        when(restRequest.param("queue")).thenReturn("testQueue");
        when(restRequest.content()).thenReturn(buffer);

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
