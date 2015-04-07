package org.mitallast.queue.transport;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.transport.TransportChannel;
import org.mitallast.queue.transport.transport.TransportFrame;

import java.util.Random;

public class TransportController extends AbstractComponent {

    @Inject
    public TransportController(Settings settings) {
        super(settings);
    }

    public void dispatchRequest(TransportChannel channel, TransportFrame request) throws Exception {
        Random random = new Random();
        ByteBuf buffer = Unpooled.buffer(random.nextInt(400) + 400);
        byte[] data = buffer.array();
        random.nextBytes(data);
        buffer.writerIndex(buffer.writerIndex() + data.length);
        TransportFrame response = TransportFrame.of(
            request.getVersion(),
            request.getRequest(),
            data.length,
            buffer
        );
        channel.send(response);
    }
}
