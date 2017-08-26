package org.mitallast.queue.common.file;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.typesafe.config.Config;

import java.io.*;
import java.nio.file.*;
import java.util.Iterator;
import java.util.stream.Stream;

public class FileService {
    private final File root;

    @Inject
    public FileService(Config config) {
        File path = new File(config.getString("node.path"));
        root = new File(path, config.getString("transport.port")).getAbsoluteFile();
        if (!root.exists() && !root.mkdirs()) {
            throw new IOError(new IOException("error create directory: " + root));
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
                    throw new IOError(new IOException("Error create directory " + resource.getParentFile()));
                }
            }
            try {
                if (!resource.createNewFile()) {
                    throw new IOError(new IOException("Error create file " + resource));
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
        return resource;
    }

    public File temporary(String service, String prefix, String suffix) {
        Path servicePath = service(service).toPath();
        try {
            return Files.createTempFile(servicePath, prefix, suffix).toFile();
        } catch (IOException e) {
            throw new IOError(e);
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
            throw new IOError(e);
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
            throw new IOError(e);
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
            throw new IOError(new IOException("Error delete file " + file));
        }
    }

    public void move(File tmp, File dest) {
        try {
            Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public DataOutputStream output(File file) {
        return output(file, false);
    }

    public DataOutputStream output(File file, boolean append) {
        try {
            FileOutputStream outputStream = new FileOutputStream(file, append);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
            return new DataOutputStream(bufferedOutputStream);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public DataInputStream input(File file) {
        try {
            FileInputStream inputStream = new FileInputStream(file);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            return new DataInputStream(bufferedInputStream);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
