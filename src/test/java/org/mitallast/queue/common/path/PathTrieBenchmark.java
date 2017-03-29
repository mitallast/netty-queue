package org.mitallast.queue.common.path;

import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;

import java.util.HashMap;
import java.util.Map;

public class PathTrieBenchmark extends BaseTest {
    private PathTrie<String> pathTrie;
    private Map<String, String> params;

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
    public void testRetrieveRoot() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            pathTrie.retrieve("/", params);
        }
        long end = System.currentTimeMillis();
        printQps("retrieve /", 10000000, start, end);
    }

    @Test
    public void testRetrieveStats() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            pathTrie.retrieve("/_stats", params);
        }
        long end = System.currentTimeMillis();
        printQps("retrieve /_stats", 10000000, start, end);
    }

    @Test
    public void testRetrieveQueue() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            pathTrie.retrieve("/queue", params);
        }
        long end = System.currentTimeMillis();
        printQps("retrieve /queue", 10000000, start, end);
    }

    @Test
    public void testRetrieveQueueStats() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            pathTrie.retrieve("/queue/_stats", params);
        }
        long end = System.currentTimeMillis();
        printQps("retrieve /queue/_stats", 10000000, start, end);
    }

    @Test
    public void testRetrieveQueueMessage() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            pathTrie.retrieve("/queue/message", params);
        }
        long end = System.currentTimeMillis();
        printQps("retrieve /queue/message", 10000000, start, end);
    }

    @Test
    public void testRetrieveQueueMessageUuid() throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            pathTrie.retrieve("/queue/message/uuid", params);
        }
        long end = System.currentTimeMillis();
        printQps("retrieve /queue/message/uuid", 10000000, start, end);
    }
}
