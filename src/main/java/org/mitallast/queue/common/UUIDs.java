package org.mitallast.queue.common;

import com.eaio.uuid.UUIDGen;

import java.util.UUID;

public class UUIDs {

    public static UUID generateRandom() {
        return new UUID(UUIDGen.newTime(), UUIDGen.getClockSeqAndNode());
    }
}
