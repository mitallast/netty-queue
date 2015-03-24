package org.mitallast.queue.common;

import com.eaio.uuid.UUIDGen;
import io.netty.handler.codec.AsciiString;

import java.util.UUID;

public class UUIDs {

    public static UUID generateRandom() {
        return new UUID(UUIDGen.newTime(), UUIDGen.getClockSeqAndNode());
    }

    public static UUID fromString(CharSequence sequence) {
        AsciiString name = AsciiString.of(sequence);
        AsciiString[] components = name.split('-');
        if (components.length != 5)
            throw new IllegalArgumentException("Invalid UUID string: " + name);


        long mostSigBits = components[0].parseLong(16);
        mostSigBits <<= 16;
        mostSigBits |= components[1].parseLong(16);
        mostSigBits <<= 16;
        mostSigBits |= components[2].parseLong(16);

        long leastSigBits = components[3].parseLong(16);
        leastSigBits <<= 48;
        leastSigBits |= components[4].parseLong(16);

        return new UUID(mostSigBits, leastSigBits);
    }
}
