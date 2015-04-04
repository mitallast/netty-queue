package org.mitallast.queue.common.path;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PathTrieTest {

    private PathTrie<String> pathTrie;
    private Map<String, CharSequence> params;

    @Before
    public void setUp() throws Exception {
        pathTrie = new PathTrie<>();
        params = new HashMap<>();
    }

    @Test
    public void testRoot() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/foo", "foo");
        pathTrie.prettyPrint();

        Assert.assertEquals("root", pathTrie.retrieve("/", params));
    }

    @Test
    public void testNamed() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/{named}", "{named}");
        pathTrie.prettyPrint();

        Assert.assertEquals("{named}", pathTrie.retrieve("/foo", params));
        Assert.assertTrue(params.containsKey("named"));
        Assert.assertNotNull(params.get("named"));
        Assert.assertFalse(params.get("named").length() == 0);
        Assert.assertEquals("foo", params.get("named").toString());
    }

    @Test
    public void testNamedAndAll() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/_all", "_all");
        pathTrie.insert("/{named}", "{named}");
        pathTrie.prettyPrint();

        Assert.assertEquals("{named}", pathTrie.retrieve("/foo", params));
        Assert.assertTrue(params.containsKey("named"));
        Assert.assertNotNull(params.get("named"));
        Assert.assertFalse(params.get("named").length() == 0);
        Assert.assertEquals("foo", params.get("named").toString());
    }

    @Test
    public void testAllAndNamed() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/_all", "_all");
        pathTrie.insert("/{named}", "{named}");
        pathTrie.prettyPrint();

        Assert.assertEquals("_all", pathTrie.retrieve("/_all", params));
        Assert.assertTrue(params.isEmpty());
    }

    @Test
    public void testLevel() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/foo", "one");
        pathTrie.insert("/foo/foo", "two");
        pathTrie.insert("/foo/foo/foo", "three");
        pathTrie.prettyPrint();

        Assert.assertEquals("root", pathTrie.retrieve("/", params));
        Assert.assertEquals("one", pathTrie.retrieve("/foo", params));
        Assert.assertEquals("two", pathTrie.retrieve("/foo/foo", params));
        Assert.assertEquals("three", pathTrie.retrieve("/foo/foo/foo", params));

        Assert.assertTrue(params.isEmpty());
    }

    @Test
    public void testRoute() {
        pathTrie.insert("/", "1");
        pathTrie.insert("/_stats", "2");
        pathTrie.insert("/{queue}", "3");
        pathTrie.insert("/{queue}/_stats", "4");
        pathTrie.insert("/{queue}/message", "5");
        pathTrie.insert("/{queue}/message/{uuid}", "6");

        params.clear();
        Assert.assertEquals("1", pathTrie.retrieve("/", params));
        Assert.assertTrue(params.isEmpty());

        params.clear();
        Assert.assertEquals("2", pathTrie.retrieve("/_stats", params));
        Assert.assertTrue(params.isEmpty());

        params.clear();
        Assert.assertEquals("3", pathTrie.retrieve("/queue", params));
        Assert.assertFalse(params.isEmpty());
        Assert.assertEquals("queue", params.get("queue").toString());

        params.clear();
        Assert.assertEquals("4", pathTrie.retrieve("/queue/_stats", params));
        Assert.assertFalse(params.isEmpty());
        Assert.assertEquals("queue", params.get("queue").toString());

        params.clear();
        Assert.assertEquals("5", pathTrie.retrieve("/queue/message", params));
        Assert.assertFalse(params.isEmpty());
        Assert.assertEquals("queue", params.get("queue").toString());

        params.clear();
        Assert.assertEquals("6", pathTrie.retrieve("/queue/message/uuid", params));
        Assert.assertFalse(params.isEmpty());
        Assert.assertEquals("queue", params.get("queue").toString());
        Assert.assertEquals("uuid", params.get("uuid").toString());
    }
}
