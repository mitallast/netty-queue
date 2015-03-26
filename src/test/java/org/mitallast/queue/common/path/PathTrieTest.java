package org.mitallast.queue.common.path;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PathTrieTest {

    private PathTrie<String> pathTrie;
    private Map<String, List<String>> params;

    @Before
    public void setUp() throws Exception {
        pathTrie = new PathTrie<>();
        params = new HashMap<>();
    }

    @Test
    public void testRoot() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/foo", "foo");
        Assert.assertEquals("root", pathTrie.retrieve("/", params));
    }

    @Test
    public void testNamed() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/{named}", "{named}");

        Assert.assertEquals("{named}", pathTrie.retrieve("/foo", params));
        Assert.assertTrue(params.containsKey("named"));
        Assert.assertNotNull(params.get("named"));
        Assert.assertFalse(params.get("named").isEmpty());
        Assert.assertEquals("foo", params.get("named").get(0));
    }

    @Test
    public void testNamedAndAll() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/_all", "_all");
        pathTrie.insert("/{named}", "{named}");

        Assert.assertEquals("{named}", pathTrie.retrieve("/foo", params));
        Assert.assertTrue(params.containsKey("named"));
        Assert.assertNotNull(params.get("named"));
        Assert.assertFalse(params.get("named").isEmpty());
        Assert.assertEquals("foo", params.get("named").get(0));
    }

    @Test
    public void testAllAndNamed() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/_all", "_all");
        pathTrie.insert("/{named}", "{named}");

        Assert.assertEquals("_all", pathTrie.retrieve("/_all", params));
        Assert.assertTrue(params.isEmpty());
    }

    @Test
    public void testLevel() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/foo", "one");
        pathTrie.insert("/foo/foo", "two");
        pathTrie.insert("/foo/foo/foo", "three");

        Assert.assertEquals("root", pathTrie.retrieve("/", params));
        Assert.assertEquals("one", pathTrie.retrieve("/foo", params));
        Assert.assertEquals("two", pathTrie.retrieve("/foo/foo", params));
        Assert.assertEquals("three", pathTrie.retrieve("/foo/foo/foo", params));

        Assert.assertTrue(params.isEmpty());
    }
}
