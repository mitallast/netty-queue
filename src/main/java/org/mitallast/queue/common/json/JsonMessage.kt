package org.mitallast.queue.common.json

import com.fasterxml.jackson.databind.JsonNode
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import java.io.DataInput
import java.io.DataOutput
import java.io.OutputStream

class CountingOutputStream : OutputStream() {
    var size = 0

    override fun write(b: Int) {
        size++
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        size += len
    }
}

class JsonMessage : Message {

    val json: JsonNode

    constructor(json: String) {
        this.json = mapper.readTree(json)
    }

    constructor(json: JsonNode) {
        this.json = json
    }

    companion object {
        private val mapper = JsonService.mapper
        val codec: Codec<JsonMessage> = object : Codec<JsonMessage> {
            override fun read(stream: DataInput): JsonMessage {
                val parser = mapper.factory.createParser(stream)
                val json = mapper.readTree<JsonNode>(parser)
                return JsonMessage(json)
            }

            override fun write(stream: DataOutput, value: JsonMessage) {
                val generator = mapper.factory.createGenerator(stream)
                mapper.writeValue(generator, value.json)
            }

            override fun size(value: JsonMessage): Int {
                val stream = CountingOutputStream()
                val generator = mapper.factory.createGenerator(stream)
                mapper.writeValue(generator, value.json)
                return stream.size
            }
        }
    }
}
