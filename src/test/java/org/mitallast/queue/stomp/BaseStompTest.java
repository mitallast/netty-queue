package org.mitallast.queue.stomp;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.stomp.DefaultStompFrame;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.junit.After;
import org.junit.Before;
import org.mitallast.queue.common.BaseQueueTest;
import org.mitallast.queue.stomp.transport.StompClient;

import java.util.ArrayList;
import java.util.List;

abstract public class BaseStompTest extends BaseQueueTest {

    private final List<StompClient> clients = new ArrayList<>();
    private StompClient client;

    @Before
    public void setUpStomp() throws Exception {
        client = createStompClient();
    }

    @After
    public void tearDownStomp() {
        for (StompClient client : clients) {
            client.stop();
        }
    }

    public StompClient stompClient() {
        return client;
    }

    public synchronized StompClient createStompClient() throws Exception {
        StompClient client = new StompClient(settings());
        client.start();
        clients.add(client);

        StompFrame connect = new DefaultStompFrame(StompCommand.CONNECT);
        connect.headers().set(StompHeaders.ACCEPT_VERSION, "1.2");
        connect.headers().set(StompHeaders.RECEIPT, "connect");

        StompFrame response = client.send(connect).get();
        assert response.command().equals(StompCommand.CONNECTED);
        assert response.headers().get(StompHeaders.RECEIPT_ID).equals("connect");

        return client;
    }

    public StompFrame sendFrame() {
        return sendFrame(randomUUID().toString());
    }

    public StompFrame sendFrame(String string) {
        return sendFrame(string.getBytes(getUTF8()));
    }

    public StompFrame sendFrame(byte[] data) {
        String receipt = randomUUID().toString();
        StompFrame frame = new DefaultStompFrame(StompCommand.SEND, Unpooled.wrappedBuffer(data));
        frame.headers().set(StompHeaders.DESTINATION, queueName());
        frame.headers().set(StompHeaders.CONTENT_TYPE, "text");
        frame.headers().set(StompHeaders.RECEIPT, receipt);
        frame.headers().setInt(StompHeaders.CONTENT_LENGTH, data.length);
        return frame;
    }
}
