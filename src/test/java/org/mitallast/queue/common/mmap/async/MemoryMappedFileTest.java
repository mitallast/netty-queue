package org.mitallast.queue.common.mmap.async;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.mmap.MemoryMappedPage;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

public class MemoryMappedFileTest extends BaseTest {

    private File file;
    private MemoryMappedFileImpl mappedFile;

    @Before
    public void setUp() throws Exception {
        file = testFolder.newFile();
        mappedFile = new MemoryMappedFileImpl(file, 4096, 10);
    }

    @After
    public void tearDown() throws Exception {
        mappedFile.close();
    }

    @Test
    public void testLong() throws Exception {
        for (long offset = 0; offset < 65536; offset += 8) {
            mappedFile.putLong(offset, offset);
            long actual = mappedFile.getLong(offset).get();
            assert actual == offset;
        }
    }

    @Test
    public void testLongWithIntOffset() throws Exception {
        for (long offset = 4; offset < 65536; offset += 8) {
            mappedFile.putLong(offset, offset);
            long actual = mappedFile.getLong(offset).get();
            assert actual == offset;
        }
    }

    @Test
    public void testInt() throws Exception {
        for (long offset = 0; offset < 65536; offset += 4) {
            mappedFile.putInt(offset, (int) offset);
            long actual = mappedFile.getInt(offset).get();
            assert actual == offset;
        }
    }

    @Test
    public void testBytes() throws Exception {
        byte[] bytesExpected = new byte[128];
        byte[] bytesActual = new byte[bytesExpected.length];
        byte counter = 0;
        for (long offset = 0; offset < 65536; offset += bytesExpected.length) {
            for (int i = 0; i < bytesExpected.length; i++) {
                bytesExpected[i] = (byte) (counter * 3 + counter);
            }
            mappedFile.putBytes(offset, bytesExpected);
            mappedFile.getBytes(offset, bytesActual);
            assert Arrays.equals(bytesExpected, bytesActual);
        }
    }

    @Test
    public void testByteBuf() throws Exception {
        final int bytes = 128;
        Random random = new Random();
        for (long offset = 0; offset < 65536; offset += bytes) {

            ByteBuf expected = Unpooled.buffer(bytes);
            ByteBuf actual = Unpooled.buffer(bytes);

            expected.resetWriterIndex();
            random.nextBytes(expected.array());
            expected.writerIndex(expected.writerIndex() + bytes);

            expected.resetReaderIndex();
            mappedFile.putBytes(offset, expected, bytes).get();

            actual.resetReaderIndex();
            actual.resetWriterIndex();
            mappedFile.getBytes(offset, actual, bytes).get();

            expected.resetReaderIndex();
            actual.resetReaderIndex();
            for (int i = 0; i < bytes; i++) {
                assert expected.readByte() == actual.readByte();
            }
        }
    }

    @Test
    public void testReopen() throws Exception {
        byte[] bytesExpected = new byte[128];
        byte[] bytesActual = new byte[bytesExpected.length];
        byte counter = 31;
        for (int i = 0; i < bytesExpected.length; i++) {
            bytesExpected[i] = (byte) (counter * 3 + counter);
        }
        mappedFile.putBytes(0, bytesExpected);
        mappedFile.flush();
        MemoryMappedFile reopenedMappedFile = new MemoryMappedFileImpl(file, 4096, 10);
        reopenedMappedFile.getBytes(0, bytesActual);
        assert Arrays.equals(bytesExpected, bytesActual);
    }

    @Test
    public void testBytesOverPageSize() throws Exception {
        byte[] bytesExpected = new byte[4096 * 3];
        byte[] bytesActual = new byte[bytesExpected.length];
        byte counter = 31;
        for (int i = 0; i < bytesExpected.length; i++) {
            counter = (byte) (counter * 3 + counter);
            bytesExpected[i] = counter;
        }
        mappedFile.putBytes(125, bytesExpected);
        mappedFile.getBytes(125, bytesActual);
        assert Arrays.equals(bytesExpected, bytesActual);
    }

    @Test
    public void testByteBufOverPageSize() throws Exception {
        final int bytes = 4096 * 3;
        ByteBuf expected = Unpooled.buffer(bytes);
        ByteBuf actual = Unpooled.buffer(bytes);

        new Random().nextBytes(expected.array());
        expected.writerIndex(expected.writerIndex() + bytes);
        expected.resetReaderIndex();
        mappedFile.putBytes(125, expected, bytes).get();
        mappedFile.getBytes(125, actual, bytes).get();

        expected.resetReaderIndex();
        actual.resetReaderIndex();
        for (int i = 0; i < bytes; i++) {
            assert expected.readByte() == actual.readByte() : i;
        }
    }

    @Test
    public void testPageConcurrent() throws Exception {
        executeConcurrent((thread, concurrency) -> {
            Random random = new Random();
            for (long i = 0; i < 1000; i++) {
                try {
                    long offset = (long) random.nextInt(8192) * 8;
                    MemoryMappedPage mappedPage = mappedFile.getPage(offset).get();
                    mappedPage.getInt(offset);
                    mappedFile.releasePage(mappedPage);
                } catch (Exception e) {
                    assert false : e;
                }
            }
        });
    }

    @Test
    public void testEmpty() throws Exception {
        Assert.assertTrue(mappedFile.isEmpty());
        mappedFile.putLong(0, 0);
        Assert.assertFalse(mappedFile.isEmpty());
    }
}
