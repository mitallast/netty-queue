package org.mitallast.queue.common.mmap.async;

import io.netty.buffer.ByteBuf;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface MemoryMappedFile extends Closeable {

    File getFile();

    void delete() throws IOException;

    SmartFuture<Boolean> putLong(long offset, long value) throws IOException;

    SmartFuture<Long> getLong(long offset) throws IOException;

    SmartFuture<Boolean> putInt(long offset, int value) throws IOException;

    SmartFuture<Integer> getInt(long offset) throws IOException;

    SmartFuture<Boolean> getBytes(long offset, ByteBuf buffer, int length) throws IOException;

    SmartFuture<Boolean> getBytes(long offset, byte[] data) throws IOException;

    SmartFuture<Boolean> getBytes(final long offset, byte[] data, final int start, final int length) throws IOException;

    SmartFuture<Boolean> putBytes(long offset, byte[] data) throws IOException;

    SmartFuture<Boolean> putBytes(long offset, ByteBuf byteBuf) throws IOException;

    SmartFuture<Boolean> putBytes(long offset, ByteBuf byteBuf, int length) throws IOException;

    SmartFuture<Boolean> putBytes(final long offset, final byte[] data, final int start, final int length) throws IOException;

    long length() throws IOException;

    boolean isEmpty() throws IOException;

    void flush() throws IOException;
}
