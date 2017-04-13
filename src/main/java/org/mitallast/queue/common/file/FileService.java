package org.mitallast.queue.common.file;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.stream.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

public class FileService {

    private final StreamService streamService;
    private final File root;

    @Inject
    public FileService(Config config, StreamService streamService) throws IOException {
        this.streamService = streamService;
        File path = new File(config.getString("node.path"));
        root = new File(path, config.getString("transport.port")).getAbsoluteFile();
        if (!root.exists() && !root.mkdirs()) {
            throw new IOException("error create directory: " + root);
        }
    }

    public File service(String service) throws IOException {
        Path rootPath = root.toPath().normalize();
        Path servicePath = Paths.get(root.getPath(), service).normalize();
        Preconditions.checkArgument(servicePath.startsWith(rootPath), "service path");

        return servicePath.toFile();
    }

    public File resource(String service, String key) throws IOException {
        Path servicePath = service(service).toPath();

        Path filePath = Paths.get(servicePath.toString(), key).normalize();
        Preconditions.checkArgument(filePath.startsWith(servicePath), "resource path");

        File resource = filePath.toFile();
        if (!resource.exists()) {
            if (!resource.getParentFile().exists()) {
                if (!resource.getParentFile().mkdirs()) {
                    throw new IOException("Error create directory " + resource.getParentFile());
                }
            }
            if (!resource.createNewFile()) {
                throw new IOException("Error create file " + resource);
            }
        }
        return resource;
    }

    public File temporary(String service, String prefix, String suffix) throws IOException {
        Path servicePath = service(service).toPath();

        return Files.createTempFile(servicePath, prefix, suffix).toFile();
    }

    public Stream<Path> resources(String service) throws IOException {
        Path servicePath = service(service).toPath();
        return Files.walk(servicePath)
            .filter(path -> path.toFile().isFile())
            .map(servicePath::relativize);
    }

    public Stream<Path> resources(String service, String prefix) throws IOException {
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher(prefix);

        Path servicePath = service(service).toPath();
        if (!servicePath.toFile().exists()) {
            return Stream.empty();
        }
        return Files.walk(servicePath)
            .filter(path -> path.toFile().isFile())
            .map(servicePath::relativize)
            .filter(matcher::matches);
    }

    public <T extends Streamable> Optional<T> read(String service, String key, StreamableReader<T> reader) throws IOException {
        File resource = resource(service, key);
        if (resource.length() == 0) {
            return Optional.empty();
        } else {
            try (StreamInput input = streamService.input(resource)) {
                return Optional.of(input.readStreamable(reader));
            }
        }
    }

    public void write(String service, String key, Streamable streamable) throws IOException {
        File resource = resource(service, key);
        try (StreamOutput output = streamService.output(resource)) {
            output.writeStreamable(streamable);
        }
    }

    public void delete(String service) throws IOException {
        Iterator<File> iterator = Files.walk(service(service).toPath())
            .map(Path::toFile)
            .sorted((o1, o2) -> -o1.compareTo(o2))
            .iterator();

        while (iterator.hasNext()) {
            File next = iterator.next();
            delete(next);
        }
    }

    public void delete(File file) throws IOException {
        if (file.exists() && !file.delete()) {
            throw new IOException("Error delete file " + file);
        }
    }
}
