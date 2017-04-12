package org.mitallast.queue.common.stream;

import com.google.common.collect.ImmutableSet;
import com.typesafe.config.ConfigFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.json.JsonStreamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class StreamTest extends BaseTest {

    private StreamService streamService;
    private ByteBuf buffer;
    private StreamInput input;
    private StreamOutput output;

    @Before
    public void setUp() throws Exception {
        streamService = new InternalStreamService(ImmutableSet.of(
            StreamableRegistry.of(TestStreamable.class, TestStreamable::new, 1)
        ));
        buffer = Unpooled.buffer();
    }

    @After
    public void tearDown() throws Exception {
        input.close();
        output.close();
        buffer.release();
    }

    @Test
    public void testClass() throws Exception {
        output = streamService.output(buffer);
        output.writeClass(TestStreamable.class);
        output.writeClass(TestStreamable.class);
        input = streamService.input(buffer);
        TestStreamable streamable = input.readStreamable();
        TestStreamable streamable2 = input.readStreamable();
    }

    @Test
    public void testJson() throws Exception {
        output = streamService.output(buffer);
        JsonStreamable json = new JsonStreamable("{}");
        output.writeStreamable(json);
        output.writeStreamable(json);
        output.writeStreamable(json);
        output.close();

        input = streamService.input(buffer);
        JsonStreamable json2;
        json2 = input.readStreamable(JsonStreamable::new);
        json2 = input.readStreamable(JsonStreamable::new);
        json2 = input.readStreamable(JsonStreamable::new);

        Assert.assertEquals(json.json(), json2.json());
    }

    @Test
    public void testText() throws Exception {
        output = streamService.output(buffer);
        output.writeText("localhost");
        output.writeText("hello");
        output.writeText("123124");
        output.close();

        input = streamService.input(buffer);
        Assert.assertEquals("localhost", input.readText());
        Assert.assertEquals("hello", input.readText());
        Assert.assertEquals("123124", input.readText());
    }

    @Test
    public void testDiscoveryNode() throws Exception {
        output = streamService.output(buffer);
        output.writeStreamable(new DiscoveryNode("localhost", 8800));
        output.writeStreamable(new DiscoveryNode("localhost", 8801));
        output.writeStreamable(new DiscoveryNode("localhost", 8802));
        output.close();

        input = streamService.input(buffer);
        Assert.assertEquals(new DiscoveryNode("localhost", 8800), input.readStreamable(DiscoveryNode::new));
        Assert.assertEquals(new DiscoveryNode("localhost", 8801), input.readStreamable(DiscoveryNode::new));
        Assert.assertEquals(new DiscoveryNode("localhost", 8802), input.readStreamable(DiscoveryNode::new));
    }

    public static class TestStreamable implements Streamable {

        public TestStreamable() {
        }

        public TestStreamable(StreamInput streamInput) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {

        }
    }
}
