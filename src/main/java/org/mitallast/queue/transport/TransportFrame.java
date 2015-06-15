package org.mitallast.queue.transport;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streams;

public class TransportFrame {

    public final static int HEADER_SIZE = 2 + 4 + 8 + 4;

    // header
    private Version version;
    private long request;
    private int size;
    // content
    private ByteBuf content;

    private TransportFrame(Version version, long request, int size, ByteBuf content) {
        this.version = version;
        this.request = request;
        this.size = size;
        this.content = content;
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

    public StreamInput inputStream() {
        return Streams.input(content, size);
    }

    public StreamOutput outputStream() {
        return Streams.output(content);
    }

    public boolean isPing() {
        return size <= 0;
    }

    public static TransportFrame of() {
        return of(Version.CURRENT);
    }

    public static TransportFrame of(long request) {
        return of(Version.CURRENT, request);
    }

    public static TransportFrame of(Version version) {
        return of(version, 0);
    }

    public static TransportFrame of(Version version, long request) {
        return of(version, request, 0, null);
    }

    public static TransportFrame of(long request, ByteBuf content) {
        return of(Version.CURRENT, request, content);
    }

    public static TransportFrame of(ByteBuf content) {
        return of(Version.CURRENT, 0, content.readableBytes(), content);
    }

    public static TransportFrame of(Version version, long request, ByteBuf content) {
        return of(version, request, content.readableBytes(), content);
    }

    public static TransportFrame of(Version version, long request, int size, ByteBuf content) {
        return new TransportFrame(version, request, size, content);
    }

    public static TransportFrame of(TransportFrame frame) {
        return of(frame.getVersion(), frame.getRequest(), frame.getSize(), frame.getContent());
    }
}
