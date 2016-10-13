package org.mitallast.queue.transport.netty.codec;

import com.google.common.base.Preconditions;
import org.mitallast.queue.Version;

public class TransportFrame {
    private final Version version;
    private final long request;

    protected TransportFrame(Version version, long request) {
        Preconditions.checkNotNull(version);
        this.version = version;
        this.request = request;
    }

    public boolean streamable() {
        return false;
    }

    public Version version() {
        return version;
    }

    public long request() {
        return request;
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
        return new TransportFrame(version, request);
    }
}
