package org.mitallast.queue.common.xstream.json;

import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.strings.Strings;

public class JsonXStreamParserTest extends BaseTest {
    @Test
    public void testReadObjectNull() throws Exception {
        assertCopy("{\"field\":null}");
    }

    @Test
    public void testReadObjectString() throws Exception {
        assertCopy("{\"field\":\"value\"}");
    }

    @Test
    public void testReadObjectInt() throws Exception {
        assertCopy("{\"field\":123}");
    }

    @Test
    public void testReadObjectFloat() throws Exception {
        assertCopy("{\"field\":123.123123}");
    }

    @Test
    public void testReadObjectBoolean() throws Exception {
        assertCopy("{\"field\":true}");
        assertCopy("{\"field\":false}");
    }

    @Test
    public void testReadObjectNested() throws Exception {
        assertCopy("{\"field\":{\"foo\":\"bar\"}}");
    }

    @Test
    public void testReadObjectArray() throws Exception {
        assertCopy("{\"field\":[]}");
        assertCopy("{\"field\":[123]}");
        assertCopy("{\"field\":[null]}");
        assertCopy("{\"field\":[false]}");
        assertCopy("{\"field\":[\"foo\"]}");
        assertCopy("{\"field\":[[]]}");
        assertCopy("{\"field\":[{}]}");
        assertCopy("{\"field\":[[[]]]}");
    }

    @Test
    public void testReadArrayNull() throws Exception {
        assertCopy("[null]");
        assertCopy("[null,null]");
    }

    @Test
    public void testReadArrayString() throws Exception {
        assertCopy("[\"foo\"]");
        assertCopy("[\"foo\",\"bar\"]");
    }

    @Test
    public void testReadArrayInt() throws Exception {
        assertCopy("[123]");
        assertCopy("[123,234]");
    }

    @Test
    public void testReadArrayFloat() throws Exception {
        assertCopy("[0.123]");
        assertCopy("[0.123,0.234]");
    }

    @Test
    public void testReadArrayBoolean() throws Exception {
        assertCopy("[false]");
        assertCopy("[true]");
        assertCopy("[false,true]");
    }

    @Test
    public void testReadArrayObject() throws Exception {
        assertCopy("[{}]");
        assertCopy("[{},{}]");
        assertCopy("[{\"foo\":\"foo\"}]");
        assertCopy("[{\"foo\":\"foo\"},{\"bar\":\"bar\"}]");
    }

    private void assertCopy(String expected) throws Exception {
        JsonXStreamParser parser = (JsonXStreamParser) JsonXStream.jsonXContent.createParser(expected);
        parser.nextToken();
        ByteBuf buffer = parser.rawBytes();
        Assert.assertEquals(expected, buffer.toString(Strings.UTF8));
    }
}
