package org.mitallast.queue.blob;

import com.google.inject.Inject;
import org.mitallast.queue.common.file.FileService;

import java.io.*;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class LocalBlobStorageService implements BlobStorageService {

    private final FileService fileService;

    @Inject
    public LocalBlobStorageService(FileService fileService) throws IOException {
        this.fileService = fileService;
    }

    @Override
    public void putObject(String key, InputStream input) throws IOException {
        File objectFile = fileService.resource("blob", key);
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
        return new FileInputStream(fileService.resource("blob", key));
    }

    @Override
    public List<String> listObjects() throws IOException {
        return fileService.resources("blob")
                .map(Path::toString)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> listObjects(String prefix) throws IOException {
        return fileService.resources("blob", prefix)
                .map(Path::toString)
                .collect(Collectors.toList());
    }
}
