package org.mitallast.queue.blob;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface BlobStorageService {

    void putObject(String key, InputStream input) throws IOException;

    InputStream getObject(String key) throws IOException;

    List<String> listObjects() throws IOException;

    List<String> listObjects(String prefix) throws IOException;
}
