package org.mitallast.queue.blob;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.unitils.util.ReaderInputStream;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

public class LocalBlobStorageServiceTest extends BaseTest {

    private BlobStorageService blobStorageService;

    @Before
    public void setUp() throws Exception {
        Config config = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
            .put("node.name", "test")
            .put("blob.path", testFolder.getRoot().getAbsolutePath())
            .build());
        blobStorageService = new LocalBlobStorageService(config);
    }

    @Test
    public void testPutAndGet() throws Exception {
        blobStorageService.putObject("test", new ReaderInputStream(new StringReader("test")));
        try (InputStream inputStream = blobStorageService.getObject("test")) {
            String string = CharStreams.toString(new InputStreamReader(inputStream));
            Assert.assertEquals("test", string);
        }
    }
}
