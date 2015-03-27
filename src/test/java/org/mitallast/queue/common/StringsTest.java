package org.mitallast.queue.common;

import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.strings.Strings;

public class StringsTest {

    @Test
    public void testSplitStringToArray() {
        String[] strings = Strings.splitStringToArray("qweqweqw-asdfasfaf-qdvdfsvdfsb-bdfsfdb", '-');
        Assert.assertEquals(4, strings.length);
        Assert.assertEquals(strings[0], "qweqweqw");
        Assert.assertEquals(strings[1], "asdfasfaf");
        Assert.assertEquals(strings[2], "qdvdfsvdfsb");
        Assert.assertEquals(strings[3], "bdfsfdb");
    }

    @Test
    public void testSplitStringToArraySignle() {
        String[] strings = Strings.splitStringToArray("qweqweqw", '-');
        Assert.assertEquals(1, strings.length);
        Assert.assertEquals(strings[0], "qweqweqw");
    }

    @Test
    public void testSplitStringToArrayPrefix() {
        String[] strings = Strings.splitStringToArray("-qweqweqw", '-');
        Assert.assertEquals(1, strings.length);
        Assert.assertEquals(strings[0], "qweqweqw");
    }

    @Test
    public void testSplitStringToArrayPostfix() {
        String[] strings = Strings.splitStringToArray("qweqweqw-", '-');
        Assert.assertEquals(1, strings.length);
        Assert.assertEquals(strings[0], "qweqweqw");
    }

    @Test
    public void testSplitStringToArrayBoth() {
        String[] strings = Strings.splitStringToArray("-qweqweqw-", '-');
        Assert.assertEquals(1, strings.length);
        Assert.assertEquals(strings[0], "qweqweqw");
    }

    @Test
    public void testEmpty() {
        String[] strings = Strings.splitStringToArray("-", '-');
        Assert.assertEquals(0, strings.length);
    }
}
