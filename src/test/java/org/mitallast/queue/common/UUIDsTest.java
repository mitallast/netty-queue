package org.mitallast.queue.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class UUIDsTest extends BaseTest {

    @Test
    public void testToByteBuf() throws Exception {
        UUID uuid = randomUUID();

        String uuidString = UUIDs.toByteBuf(uuid).toString(Strings.UTF8);

        Assert.assertEquals(uuid.toString(), uuidString);
    }
}
