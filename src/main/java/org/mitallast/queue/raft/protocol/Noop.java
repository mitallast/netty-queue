package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;

public class Noop implements Message {
    public static final Noop INSTANCE = new Noop();
    public static final Codec<Noop> codec = Codec.of(INSTANCE);

    private Noop() {
    }
}
