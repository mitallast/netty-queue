package org.mitallast.queue.common.file;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import javaslang.control.Option;
import org.mitallast.queue.common.stream.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Iterator;
import java.util.stream.Stream;

public class FileService {

    private final StreamService streamService;
    private final File root;

    @Inject
    public FileService(Config config, StreamService streamService) {
        this.streamService = streamService;
        File path = new File(config.getString("node.path"));
        root = new File(path, config.getString("transport.port")).getAbsoluteFile();
        if (!root.exists() && !root.mkdirs()) {
            throw new FileException("error create directory: " + root);
        }
    }

    public File service(String service) {
        Path rootPath = root.toPath().normalize();
        Path servicePath = Paths.get(root.getPath(), service).normalize();
        Preconditions.checkArgument(servicePath.startsWith(rootPath), "service path");
        return servicePath.toFile();
    }

    public File resource(String service, String key) {
        Path servicePath = service(service).toPath();

        Path filePath = Paths.get(servicePath.toString(), key).normalize();
        Preconditions.checkArgument(filePath.startsWith(servicePath), "resource path");

        File resource = filePath.toFile();
        if (!resource.exists()) {
            if (!resource.getParentFile().exists()) {
                if (!resource.getParentFile().mkdirs()) {
                    throw new FileException("Error create directory " + resource.getParentFile());
                }
            }
            try {
                if (!resource.createNewFile()) {
                    throw new FileException("Error create file " + resource);
                }
            } catch (IOException e) {
                throw new FileException(e);
            }
        }
        return resource;
    }

    public File temporary(String service, String prefix, String suffix) {
        Path servicePath = service(service).toPath();
        try {
            return Files.createTempFile(servicePath, prefix, suffix).toFile();
        } catch (IOException e) {
            throw new FileException(e);
        }
    }

    public Stream<Path> resources(String service) {
        Path servicePath = service(service).toPath();
        if (!servicePath.toFile().exists()) {
            return Stream.empty();
        }
        try {
            return Files.walk(servicePath)
                .filter(path -> path.toFile().isFile())
                .map(servicePath::relativize);
        } catch (IOException e) {
            throw new FileException(e);
        }
    }

    public Stream<Path> resources(String service, String prefix) {
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher(prefix);

        Path servicePath = service(service).toPath();
        if (!servicePath.toFile().exists()) {
            return Stream.empty();
        }
        try {
            return Files.walk(servicePath)
                .filter(path -> path.toFile().isFile())
                .map(servicePath::relativize)
                .filter(matcher::matches);
        } catch (IOException e) {
            throw new FileException(e);
        }
    }

    public <T extends Streamable> Option<T> read(String service, String key, StreamableReader<T> reader) {
        File resource = resource(service, key);
        if (resource.length() == 0) {
            return Option.none();
        } else {
            try (StreamInput input = streamService.input(resource)) {
                return Option.some(input.readStreamable(reader));
            }
        }
    }

    public void write(String service, String key, Streamable streamable) {
        File resource = resource(service, key);
        try (StreamOutput output = streamService.output(resource)) {
            output.writeStreamable(streamable);
        }
    }

    public void delete(String service) {
        Path servicePath = service(service).toPath();
        if (!servicePath.toFile().exists()) {
            return;
        }
        try {
            Iterator<File> iterator = Files.walk(servicePath)
                .map(Path::toFile)
                .sorted((o1, o2) -> -o1.compareTo(o2))
                .iterator();
            while (iterator.hasNext()) {
                File next = iterator.next();
                delete(next);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void delete(File file) {
        if (file.exists() && !file.delete()) {
            throw new FileException("Error delete file " + file);
        }
    }

    public void move(File tmp, File dest) {
        try {
            Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new FileException("Error move file " + dest);
        }
    }
}
