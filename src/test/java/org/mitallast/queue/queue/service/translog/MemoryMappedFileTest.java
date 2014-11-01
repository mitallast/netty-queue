package org.mitallast.queue.queue.service.translog;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class MemoryMappedFileTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private File file;
    private MemoryMappedFile mappedFile;

    @Before
    public void setUp() throws Exception {
        file = folder.newFile();
        mappedFile = new MemoryMappedFile(new RandomAccessFile(file, "rw"), 4096, 10);
    }

    @After
    public void tearDown() throws Exception {
        mappedFile.close();
    }

    @Test
    public void testLong() throws IOException {
        for (long offset = 0; offset < 655360; offset += 8) {
            mappedFile.putLong(offset, offset);
            long actual = mappedFile.getLong(offset);
            assert actual == offset;
        }
    }

    @Test
    public void testInt() throws IOException {
        for (long offset = 0; offset < 655360; offset += 4) {
            mappedFile.putInt(offset, (int) offset);
            long actual = mappedFile.getInt(offset);
            assert actual == offset;
        }
    }

    @Test
    public void testBytes() throws IOException {
        byte[] bytesExpected = new byte[128];
        byte[] bytesActual = new byte[bytesExpected.length];
        byte counter = 0;
        for (long offset = 0; offset < 655360; offset += bytesExpected.length) {
            for (int i = 0; i < bytesExpected.length; i++) {
                bytesExpected[i] = (byte) (counter * 3 + counter);
            }
            mappedFile.putBytes(offset, bytesExpected);
            mappedFile.getBytes(offset, bytesActual);
            assert Arrays.equals(bytesExpected, bytesActual);
        }
    }

    @Test
    public void testReopen() throws IOException {
        byte[] bytesExpected = new byte[128];
        byte[] bytesActual = new byte[bytesExpected.length];
        byte counter = 0;
        for (int i = 0; i < bytesExpected.length; i++) {
            bytesExpected[i] = (byte) (counter * 3 + counter);
        }
        mappedFile.putBytes(0, bytesExpected);
        mappedFile.flush();
        MemoryMappedFile reopenedMappedFile = new MemoryMappedFile(new RandomAccessFile(file, "rw"), 4096, 10);
        reopenedMappedFile.getBytes(0, bytesActual);
        assert Arrays.equals(bytesExpected, bytesActual);
    }

    @Test
    public void testBytesOverPageSize() throws IOException {
        byte[] bytesExpected = new byte[4096 * 3];
        byte[] bytesActual = new byte[bytesExpected.length];
        byte counter = 0;
        for (int i = 0; i < bytesExpected.length; i++) {
            bytesExpected[i] = (byte) (counter * 3 + counter);
        }
        mappedFile.putBytes(125, bytesExpected);
        mappedFile.getBytes(125, bytesActual);
        assert Arrays.equals(bytesExpected, bytesActual);
    }
}
