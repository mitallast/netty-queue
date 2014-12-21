package org.mitallast.queue.stomp.action;

import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.junit.Test;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageType;
import org.mitallast.queue.stomp.BaseStompTest;

public class StompSendActionTest extends BaseStompTest {

    @Test
    public void test() throws Exception {
        createQueue();
        StompFrame send = sendFrame();
        send.content().retain();

        StompFrame response = stompClient().send(send).get();
        assert response.command().equals(StompCommand.RECEIPT);
        assert response.headers().get(StompHeaders.RECEIPT_ID).equals(send.headers().get(StompHeaders.RECEIPT));

        send.content().resetReaderIndex();

        QueueMessage message = dequeue().getMessage();
        assert message.getMessageType() == QueueMessageType.STRING;
        assert message.getSource().toString(getUTF8()).equals(send.content().toString(getUTF8())) : message.getSource().toString(getUTF8()) + " != " + send.content().toString(getUTF8());
        send.content().release();
    }
}
