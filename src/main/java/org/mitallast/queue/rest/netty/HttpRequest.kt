package org.mitallast.queue.rest.netty

import com.google.common.base.Preconditions
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufOutputStream
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.*
import io.netty.handler.codec.http.HttpResponseStatus.OK
import io.netty.handler.codec.http.HttpVersion.HTTP_1_1
import io.netty.handler.stream.ChunkedNioFile
import io.netty.handler.stream.ChunkedStream
import io.netty.util.AsciiString
import io.netty.util.CharsetUtil
import org.apache.logging.log4j.LogManager
import org.joda.time.format.DateTimeFormat
import org.mitallast.queue.common.json.JsonService
import org.mitallast.queue.rest.ResponseBuilder
import org.mitallast.queue.rest.RestRequest
import java.io.*
import java.net.URI
import java.net.URISyntaxException
import java.net.URL
import java.util.*
import java.util.zip.ZipFile
import javax.activation.MimetypesFileTypeMap

class HttpRequest(private val ctx: ChannelHandlerContext,
                  private val httpRequest: FullHttpRequest,
                  private val jsonService: JsonService) : RestRequest {

    override val httpMethod: HttpMethod = httpRequest.method()

    override val paramMap: Map<String, String>
    override val queryPath: String
    override val uri: String = httpRequest.uri()
    override val content: ByteBuf = httpRequest.content()

    init {
        val uri = httpRequest.uri()

        val pathEndPos = uri.indexOf('?')

        if (pathEndPos < 0) {
            paramMap = HashMap()
            queryPath = uri
        } else {
            val decoder = QueryStringDecoder(uri)
            queryPath = uri.substring(0, pathEndPos)
            val parameters = decoder.parameters()
            paramMap = if (parameters.isEmpty()) {
                emptyMap()
            } else {
                val map = HashMap<String, String>(parameters.size)
                parameters.entries.stream()
                    .filter { entry -> entry.value != null && !entry.value.isEmpty() }
                    .forEach { entry -> map.put(entry.key, entry.value[0]) }
                map
            }
        }
    }

    override fun param(param: String): String {
        val `val` = paramMap[param]
        return `val` ?: throw IllegalArgumentException("Param {$param} not found")
    }

    override fun hasParam(param: String): Boolean {
        return paramMap.containsKey(param)
    }

    override fun response(): ResponseBuilder {
        return HttpResponseBuilder()
    }

    private inner class HttpResponseBuilder : ResponseBuilder {
        private var status = HttpResponseStatus.OK
        private val headers = DefaultHttpHeaders(false)

        override fun status(status: Int): ResponseBuilder {
            this.status = HttpResponseStatus.valueOf(status)
            return this
        }

        override fun status(status: Int, reason: String): ResponseBuilder {
            Preconditions.checkNotNull(reason)
            this.status = HttpResponseStatus(status, reason)
            return this
        }

        override fun status(status: HttpResponseStatus): ResponseBuilder {
            Preconditions.checkNotNull(status)
            this.status = status
            return this
        }

        override fun header(name: AsciiString, value: AsciiString): ResponseBuilder {
            Preconditions.checkNotNull(name)
            Preconditions.checkNotNull(value)
            headers.add(name, value)
            return this
        }

        override fun header(name: AsciiString, value: String): ResponseBuilder {
            Preconditions.checkNotNull(name)
            Preconditions.checkNotNull(value)
            headers.add(name, value)
            return this
        }

        override fun header(name: AsciiString, value: Long): ResponseBuilder {
            Preconditions.checkNotNull(name)
            headers.add(name, value)
            return this
        }

        override fun error(error: Throwable) {
            Preconditions.checkNotNull(error)
            val buffer = ctx.alloc().buffer()
            try {
                ByteBufOutputStream(buffer).use { outputStream -> PrintWriter(OutputStreamWriter(outputStream, CharsetUtil.UTF_8)).use { printWriter -> error.printStackTrace(printWriter) } }
            } catch (e: IOException) {
                buffer.release()
                throw RuntimeException(e)
            }

            header(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN)
            data(buffer)
        }

        override fun json(json: Any) {
            Preconditions.checkNotNull(json)
            val buf = ctx.alloc().buffer()
            jsonService.serialize(buf, json)
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
            data(buf)
        }

        override fun text(content: String) {
            Preconditions.checkNotNull(content)
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN)
            data(Unpooled.copiedBuffer(content, CharsetUtil.UTF_8))
        }

        override fun bytes(content: ByteArray) {
            Preconditions.checkNotNull(content)
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.BINARY)
            data(Unpooled.wrappedBuffer(content))
        }

        override fun data(content: ByteBuf) {
            Preconditions.checkNotNull(content)
            val response = DefaultFullHttpResponse(
                HTTP_1_1, status,
                content,
                headers,
                EmptyHttpHeaders.INSTANCE
            )
            HttpUtil.setContentLength(response, content.readableBytes().toLong())
            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true)
            }
            val writeFuture = ctx.writeAndFlush(response)
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                writeFuture.addListener(ChannelFutureListener.CLOSE)
            }
        }

        override fun file(url: URL) {
            when (url.protocol) {
                "file" -> try {
                    file(File(url.toURI()))
                } catch (e: URISyntaxException) {
                    throw RuntimeException(e)
                }

                "jar" -> try {
                    val path = URI(url.path).path
                    val bangIndex = path.indexOf('!')
                    val filePath = path.substring(0, bangIndex)
                    val resourcePath = path.substring(bangIndex + 2)

                    val jar = ZipFile(filePath)
                    val entry = jar.getEntry(resourcePath)

                    val lastModified = entry.time
                    if (isModifiedSince(lastModified)) {
                        sendNotModified()
                    } else {
                        val stream = jar.getInputStream(entry)
                        val contentLength = entry.size
                        file(resourcePath, contentLength, lastModified, stream)
                    }
                } catch (e: IOException) {
                    throw RuntimeException(e)
                } catch (e: URISyntaxException) {
                    throw RuntimeException(e)
                }

                else -> try {
                    val conn = url.openConnection()
                    // otherwise the JDK will keep the connection open when we close!
                    conn.useCaches = false

                    val contentLength = conn.contentLength.toLong()
                    val lastModified = conn.lastModified
                    val stream = conn.getInputStream()

                    if (isModifiedSince(lastModified)) {
                        stream.close()
                        sendNotModified()
                    } else {
                        file(url.path, contentLength, lastModified, stream)
                    }
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }

            }
        }

        private fun file(path: String, contentLength: Long, lastModified: Long, stream: InputStream) {
            logger.info("path: {}", path)

            mimetype(path)

            header(HttpHeaderNames.CONTENT_LENGTH, contentLength)
            lastModified(lastModified)

            val response = DefaultHttpResponse(HTTP_1_1, OK, headers)

            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true)
            }
            ctx.write(response)
            val write = ctx.writeAndFlush(HttpChunkedInput(ChunkedStream(stream, 8192)))
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                write.addListener(ChannelFutureListener.CLOSE)
            }
        }

        override fun file(file: File) {
            val lastModified = file.lastModified()
            if (isModifiedSince(lastModified)) {
                sendNotModified()
            } else {
                mimetype(file.path)
                lastModified(lastModified)
                header(HttpHeaderNames.CONTENT_LENGTH, file.length())
                val response = DefaultHttpResponse(HTTP_1_1, OK, headers)

                if (HttpUtil.isKeepAlive(httpRequest)) {
                    HttpUtil.setKeepAlive(response, true)
                }
                ctx.write(response)

                try {
                    val write = ctx.writeAndFlush(HttpChunkedInput(ChunkedNioFile(file)))
                    if (!HttpUtil.isKeepAlive(httpRequest)) {
                        write.addListener(ChannelFutureListener.CLOSE)
                    }
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }

            }
        }

        private fun mimetype(path: String) {
            if (path.endsWith(".js")) {
                header(HttpHeaderNames.CONTENT_TYPE, APPLICATION_JAVASCRIPT)
            } else if (path.endsWith(".css")) {
                header(HttpHeaderNames.CONTENT_TYPE, TEXT_CSS)
            } else if (path.endsWith(".html")) {
                header(HttpHeaderNames.CONTENT_TYPE, TEXT_HTML)
            } else {
                header(HttpHeaderNames.CONTENT_TYPE, mimeTypesMap.getContentType(path))
            }
        }

        override fun empty() {
            header(HttpHeaderNames.CONTENT_LENGTH, 0L)
            val response = DefaultFullHttpResponse(
                HTTP_1_1,
                status,
                Unpooled.buffer(0),
                headers,
                EmptyHttpHeaders.INSTANCE
            )
            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true)
            }
            val write = ctx.writeAndFlush(response)
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                write.addListener(ChannelFutureListener.CLOSE)
            }
        }

        private fun lastModified(lastModified: Long) {
            header(HttpHeaderNames.LAST_MODIFIED, dateFormat.print(lastModified))
        }

        private fun isModifiedSince(lastModified: Long): Boolean {
            val ifModifiedSince = httpRequest.headers().get(HttpHeaderNames.IF_MODIFIED_SINCE)
            if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
                try {
                    val ifModifiedSinceSec = dateFormat.parseMillis(ifModifiedSince) / 1000
                    val lastModifiedSec = lastModified / 1000
                    return lastModifiedSec == ifModifiedSinceSec
                } catch (e: UnsupportedOperationException) {
                    logger.warn(e)
                    return false
                } catch (e: IllegalArgumentException) {
                    logger.warn(e)
                    return false
                }

            }
            return false
        }

        private fun sendNotModified() {
            val response = DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_MODIFIED)
            response.headers().set(HttpHeaderNames.DATE, dateFormat.print(System.currentTimeMillis()))

            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true)
            }
            val write = ctx.writeAndFlush(response)
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                write.addListener(ChannelFutureListener.CLOSE)
            }
        }
    }

    companion object {
        private val logger = LogManager.getLogger()
        private val mimeTypesMap = MimetypesFileTypeMap()

        private val APPLICATION_JAVASCRIPT = AsciiString("application/javascript")
        private val TEXT_CSS = AsciiString("text/css")
        private val TEXT_HTML = AsciiString("text/html")

        private val dateFormat = DateTimeFormat
            .forPattern("EEE, dd MMM yyyy HH:mm:ss Z")
            .withLocale(Locale.US)
            .withZoneUTC()
    }
}
