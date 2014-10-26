package org.mitallast.queue.common.path;

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
        params = new HashMap<String, List<String>>();
    }

    @Test
    public void testRoot() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/foo", "foo");
        assert pathTrie.retrieve("/", params).equals("root");
    }

    @Test
    public void testNamed() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/{named}", "{named}");

        assert pathTrie.retrieve("/foo", params).equals("{named}");
        assert params.containsKey("named");
        assert params.get("named") != null;
        assert !params.get("named").isEmpty();
        assert params.get("named").get(0).equals("foo");
    }

    @Test
    public void testNamedAndAll() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/_all", "_all");
        pathTrie.insert("/{named}", "{named}");

        assert pathTrie.retrieve("/foo", params).equals("{named}");
        assert params.containsKey("named");
        assert params.get("named") != null;
        assert !params.get("named").isEmpty();
        assert params.get("named").get(0).equals("foo");
    }

    @Test
    public void testAllAndNamed() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/_all", "_all");
        pathTrie.insert("/{named}", "{named}");

        assert pathTrie.retrieve("/_all", params).equals("_all");
        assert params.isEmpty();
    }

    @Test
    public void testLevel() {
        pathTrie.insert("/", "root");
        pathTrie.insert("/foo", "one");
        pathTrie.insert("/foo/foo", "two");
        pathTrie.insert("/foo/foo/foo", "three");

        assert pathTrie.retrieve("/", params).equals("root");
        assert pathTrie.retrieve("/foo", params).equals("one");
        assert pathTrie.retrieve("/foo/foo", params).equals("two");
        assert pathTrie.retrieve("/foo/foo/foo", params).equals("three");

        assert params.isEmpty();
    }
}
