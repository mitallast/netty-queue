package org.mitallast.queue.common.file

import com.google.common.base.Preconditions
import com.google.inject.Inject
import com.typesafe.config.Config

import java.io.*
import java.nio.file.*
import java.util.stream.Stream

class FileService @Inject
constructor(config: Config) {
    private val root: File

    init {
        val path = File(config.getString("node.path"))
        root = File(path, config.getString("transport.port")).absoluteFile
        if (!root.exists() && !root.mkdirs()) {
            throw IOException("error create directory: " + root)
        }
    }

    fun service(service: String): File {
        val rootPath = root.toPath().normalize()
        val servicePath = Paths.get(root.path, service).normalize()
        Preconditions.checkArgument(servicePath.startsWith(rootPath), "service path")
        return servicePath.toFile()
    }

    fun resource(service: String, key: String): File {
        val servicePath = service(service).toPath()

        val filePath = Paths.get(servicePath.toString(), key).normalize()
        Preconditions.checkArgument(filePath.startsWith(servicePath), "resource path")

        val resource = filePath.toFile()
        if (!resource.exists()) {
            if (!resource.parentFile.exists()) {
                if (!resource.parentFile.mkdirs()) {
                    throw IOException("Error create directory " + resource.parentFile)
                }
            }
            if (!resource.createNewFile()) {
                throw IOException("Error create file " + resource)
            }
        }
        return resource
    }

    fun temporary(service: String, prefix: String, suffix: String): File {
        val servicePath = service(service).toPath()
        return Files.createTempFile(servicePath, prefix, suffix).toFile()
    }

    fun resources(service: String): Stream<Path> {
        val servicePath = service(service).toPath()
        if (!servicePath.toFile().exists()) {
            return Stream.empty()
        }
        return Files.walk(servicePath)
            .filter { path -> path.toFile().isFile }
            .map { servicePath.relativize(it) }
    }

    fun resources(service: String, prefix: String): Stream<Path> {
        val matcher = FileSystems.getDefault().getPathMatcher(prefix)

        val servicePath = service(service).toPath()
        if (!servicePath.toFile().exists()) {
            return Stream.empty()
        }
        return Files.walk(servicePath)
            .filter { path -> path.toFile().isFile }
            .map { servicePath.relativize(it) }
            .filter { matcher.matches(it) }
    }

    fun delete(service: String) {
        val servicePath = service(service).toPath()
        if (servicePath.toFile().exists()) {
            val iterator = Files.walk(servicePath)
                .map { it.toFile() }
                .sorted { o1, o2 -> -o1.compareTo(o2) }
                .iterator()
            while (iterator.hasNext()) {
                val next = iterator.next()
                delete(next)
            }
        }
    }

    fun delete(file: File) {
        if (file.exists() && !file.delete()) {
            throw IOException("Error delete file " + file)
        }
    }

    fun move(tmp: File, dest: File) {
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING)
    }

    fun output(file: File, append: Boolean = false): DataOutputStream {
        try {
            val outputStream = FileOutputStream(file, append)
            val bufferedOutputStream = BufferedOutputStream(outputStream)
            return DataOutputStream(bufferedOutputStream)
        } catch (e: IOException) {
            throw IOError(e)
        }

    }

    fun input(file: File): DataInputStream {
        try {
            val inputStream = FileInputStream(file)
            val bufferedInputStream = BufferedInputStream(inputStream)
            return DataInputStream(bufferedInputStream)
        } catch (e: IOException) {
            throw IOError(e)
        }

    }
}
