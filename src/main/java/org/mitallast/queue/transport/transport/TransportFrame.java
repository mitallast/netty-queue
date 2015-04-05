package org.mitallast.queue.transport.transport;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.Version;

public class TransportFrame {

    public final static int HEADER_SIZE = 2 + 4 + 8 + 4;

    // header
    private Version version;
    private long request;
    private int size;
    // content
    private ByteBuf content;

    public TransportFrame() {
    }

    public TransportFrame(Version version, long request, int size, ByteBuf content) {
        this.version = version;
        this.request = request;
        this.size = size;
        this.content = content;
    }

    public static TransportFrame of(ByteBuf buffer) {
        return of(buffer, 0);
    }

    public static TransportFrame of(ByteBuf buffer, long request) {
        TransportFrame frame = new TransportFrame();
        frame.setVersion(Version.V1_0_0);
        frame.setRequest(request);
        frame.setSize(buffer.readableBytes());
        frame.setContent(buffer);
        return frame;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public long getRequest() {
        return request;
    }

    public void setRequest(long request) {
        this.request = request;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ByteBuf getContent() {
        return content;
    }

    public void setContent(ByteBuf content) {
        this.content = content;
    }
}
