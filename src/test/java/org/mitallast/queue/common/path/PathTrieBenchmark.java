package org.mitallast.queue.common.path;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseBenchmark;

import java.util.HashMap;
import java.util.Map;

public class PathTrieBenchmark extends BaseBenchmark {
    private PathTrie<String> pathTrie;
    private Map<String, CharSequence> params;

    @Before
    public void setUp() throws Exception {
        pathTrie = new PathTrie<>();
        pathTrie.insert("/", "1");
        pathTrie.insert("/_stats", "2");
        pathTrie.insert("/{queue}", "3");
        pathTrie.insert("/{queue}/_stats", "4");
        pathTrie.insert("/{queue}/message", "5");
        pathTrie.insert("/{queue}/message/{uuid}", "6");
        params = new HashMap<>();
    }

    @Test
    public void testRetrieve() throws Exception {
        for (int i = 0; i < 100000; i++) {
//        while (true) {
            Assert.assertEquals("1", pathTrie.retrieve("/", params));
            Assert.assertEquals("2", pathTrie.retrieve("/_stats", params));
            Assert.assertEquals("3", pathTrie.retrieve("/queue", params));
            Assert.assertEquals("4", pathTrie.retrieve("/queue/_stats", params));
            Assert.assertEquals("5", pathTrie.retrieve("/queue/message", params));
            Assert.assertEquals("6", pathTrie.retrieve("/queue/message/uuid", params));
        }
    }
}
