package org.mitallast.queue.common.xstream;

import io.netty.buffer.ByteBuf;

import java.io.*;

public interface XStream {
    /**
     * The type this content handles and produces.
     */
    XStreamType type();

    /**
     * Creates a new generator using the provided output stream.
     */
    XStreamBuilder createGenerator(OutputStream os) throws IOException;

    /**
     * Creates a new generator using the provided writer.
     */
    XStreamBuilder createGenerator(Writer writer) throws IOException;

    /**
     * Creates a parser over the provided string content.
     */
    XStreamParser createParser(String content) throws IOException;

    /**
     * Creates a parser over the provided input stream.
     */
    XStreamParser createParser(InputStream is) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XStreamParser createParser(byte[] data) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XStreamParser createParser(byte[] data, int offset, int length) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XStreamParser createParser(ByteBuf bytes) throws IOException;

    /**
     * Creates a parser over the provided reader.
     */
    XStreamParser createParser(Reader reader) throws IOException;
}
