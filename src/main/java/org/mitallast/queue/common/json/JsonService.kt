package org.mitallast.queue.common.json

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.ByteBufOutputStream
import io.vavr.jackson.datatype.VavrModule
import org.mitallast.queue.common.codec.UnsafeByteBufInputStream
import java.io.InputStream
import java.io.OutputStream

class JsonService {

    fun serialize(buf: ByteBuf, json: Any) {
        ByteBufOutputStream(buf).use { serialize(it, json) }
    }

    fun serialize(json: Any): String {
        return mapper.writeValueAsString(json)
    }

    fun serialize(out: OutputStream, json: Any) {
        mapper.writeValue(out, json)
    }

    fun <T> deserialize(data: String, type: Class<T>): T {
        return mapper.readValue(data, type)
    }

    fun <T> deserialize(buf: ByteBuf, type: Class<T>): T {
        UnsafeByteBufInputStream(buf).use { return deserialize(it, type) }
    }

    fun <T> deserialize(input: InputStream, type: Class<T>): T {
        return mapper.readValue(input, type)
    }

    fun <T> deserialize(data: String, type: TypeReference<T>): T {
        return mapper.readValue(data, type)
    }

    fun <T> deserialize(buf: ByteBuf, type: TypeReference<T>): T {
        UnsafeByteBufInputStream(buf).use { return deserialize(it, type) }
    }

    fun <T> deserialize(input: InputStream, type: TypeReference<T>): T {
        return mapper.readValue(input, type)
    }

    private class ConfigSerializer : JsonSerializer<Config>() {
        override fun serialize(value: Config, gen: JsonGenerator, serializers: SerializerProvider) {
            val render = value.root().render(ConfigRenderOptions.concise())
            gen.writeRawValue(render)
        }
    }

    private class ConfigDeserializer : JsonDeserializer<Config>() {
        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Config {
            val json = p.readValueAsTree<TreeNode>().toString()
            return ConfigFactory.parseString(json)
        }
    }

    private class JsonMessageSerializer : JsonSerializer<JsonMessage>() {
        override fun serialize(value: JsonMessage, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeTree(value.json)
        }
    }

    private class JsonMessageDeserializer : JsonDeserializer<JsonMessage>() {
        override fun deserialize(p: JsonParser, ctx: DeserializationContext): JsonMessage {
            val treeNode = p.readValueAsTree<JsonNode>()
            return JsonMessage(treeNode)
        }
    }

    companion object {
        val mapper: ObjectMapper

        init {
            val module = SimpleModule()
            module.addSerializer(Config::class.java, ConfigSerializer())
            module.addDeserializer(Config::class.java, ConfigDeserializer())
            module.addSerializer(JsonMessage::class.java, JsonMessageSerializer())
            module.addDeserializer(JsonMessage::class.java, JsonMessageDeserializer())

            mapper = ObjectMapper()
            mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            mapper.registerModule(module)
            mapper.registerModule(VavrModule())
            mapper.registerModule(JodaModule())
        }
    }
}
