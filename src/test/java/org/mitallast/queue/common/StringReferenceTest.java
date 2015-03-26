package org.mitallast.queue.common;

import org.junit.Assert;
import org.junit.Test;

public class StringReferenceTest extends BaseTest {
    @Test
    public void testToString() {
        String string = randomUUID().toString();
        StringReference sequence = StringReference.of(string);

        Assert.assertEquals(string, sequence.toString());
    }
}
