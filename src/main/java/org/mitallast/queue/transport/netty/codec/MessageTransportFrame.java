package org.mitallast.queue.transport.netty.codec;

import org.mitallast.queue.Version;
import org.mitallast.queue.common.stream.Streamable;

public class MessageTransportFrame implements TransportFrame {

    private final Version version;
    private final Streamable message;

    public MessageTransportFrame(Version version, Streamable message) {
        this.version = version;
        this.message = message;
    }

    @Override
    public Version version() {
        return version;
    }

    @Override
    public TransportFrameType type() {
        return TransportFrameType.MESSAGE;
    }

    @SuppressWarnings("unchecked")
    public <T extends Streamable> T message() {
        return (T) message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageTransportFrame that = (MessageTransportFrame) o;

        if (!version.equals(that.version)) return false;
        return message.equals(that.message);
    }

    @Override
    public int hashCode() {
        int result = version.hashCode();
        result = 31 * result + message.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MessageTransportFrame{" +
                "version=" + version +
                ", message=" + message +
                '}';
    }
}
