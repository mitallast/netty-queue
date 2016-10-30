package org.mitallast.queue.blob;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collectors;

public class LocalBlobStorageService extends AbstractComponent implements BlobStorageService {

    private File root;

    @Inject
    public LocalBlobStorageService(Config config) throws IOException {
        super(config.getConfig("blob"), BlobStorageService.class);
        File path = new File(this.config.getString("path"));
        root = new File(path, config.getString("node.name"));
        if (!root.exists() && !root.mkdirs()) {
            throw new IOException("error create directory: " + root);
        }
    }

    @Override
    public void putObject(String key, InputStream input) throws IOException {
        File objectFile = file(key);
        try (FileOutputStream output = new FileOutputStream(objectFile)) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) > 0) {
                output.write(buffer, 0, read);
            }
        }
    }

    @Override
    public InputStream getObject(String key) throws IOException {
        return new FileInputStream(file(key));
    }

    @Override
    public List<String> listObjects() throws IOException {
        Path rootPath = root.toPath();
        return Files.walk(rootPath)
            .map(path -> path.relativize(rootPath).toString())
            .collect(Collectors.toList());
    }

    @Override
    public List<String> listObjects(String prefix) throws IOException {
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher(prefix);
        Path rootPath = root.toPath();
        return Files.walk(rootPath)
            .map(path -> path.relativize(rootPath))
            .filter(matcher::matches)
            .map(Path::toString)
            .collect(Collectors.toList());
    }

    private File file(String key) {
        return new File(root, key);
    }
}
