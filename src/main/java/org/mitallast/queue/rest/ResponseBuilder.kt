package org.mitallast.queue.rest

import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.util.AsciiString

import java.io.File
import java.net.URL

interface ResponseBuilder {

    fun status(status: Int): ResponseBuilder

    fun status(status: Int, reason: String): ResponseBuilder

    fun status(status: HttpResponseStatus): ResponseBuilder

    fun header(name: AsciiString, value: AsciiString): ResponseBuilder

    fun header(name: AsciiString, value: String): ResponseBuilder

    fun header(name: AsciiString, value: Long): ResponseBuilder

    fun error(error: Throwable)

    fun json(json: Any)

    fun text(content: String)

    fun bytes(content: ByteArray)

    fun data(content: ByteBuf)

    fun file(url: URL)

    fun file(file: File)

    fun empty()
}
