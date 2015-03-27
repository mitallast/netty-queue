package org.mitallast.queue.common;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.strings.CharSequenceReference;

public class CharSequenceReferenceTest extends BaseTest {
    @Test
    public void testToString() {
        String string = randomUUID().toString();
        CharSequenceReference sequence = CharSequenceReference.of(string);

        Assert.assertEquals(string, sequence.toString());
    }
}
