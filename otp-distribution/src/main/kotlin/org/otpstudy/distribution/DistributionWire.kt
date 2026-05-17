package org.otpstudy.distribution

import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.serializer

/**
 * JSON payloads on the Kotlin dist wire. Primitives and [JsonElement] are supported; for structured
 * application data use [@Serializable] types via [encodeSerializable] / [decodeSerializable].
 */
object DistributionWire {
    val json: Json =
        Json {
            classDiscriminator = "type"
            ignoreUnknownKeys = true
            encodeDefaults = true
        }

    inline fun <reified T : Any> encodeSerializable(value: T): JsonElement =
        json.encodeToJsonElement(json.serializersModule.serializer<T>(), value)

    inline fun <reified T : Any> decodeSerializable(el: JsonElement): T =
        json.decodeFromJsonElement(json.serializersModule.serializer<T>(), el)

    fun encodePayload(value: Any?): JsonElement =
        when (value) {
            null -> JsonNull
            is JsonElement -> value
            is String -> JsonPrimitive(value)
            is Boolean -> JsonPrimitive(value)
            is Int -> JsonPrimitive(value)
            is Long -> JsonPrimitive(value)
            is Float -> JsonPrimitive(value)
            is Double -> JsonPrimitive(value)
            else -> encodeSerializableOrToString(value)
        }

    private fun encodeSerializableOrToString(value: Any): JsonElement =
        try {
            @Suppress("UNCHECKED_CAST")
            json.encodeToJsonElement(value as Any)
        } catch (_: SerializationException) {
            JsonPrimitive(value.toString())
        }

    fun decodePayload(el: JsonElement): Any? =
        when (el) {
            JsonNull -> null
            is JsonPrimitive -> {
                el.booleanOrNull
                    ?: el.longOrNull
                    ?: el.doubleOrNull
                    ?: el.contentOrNull
            }
            is JsonArray, is JsonObject -> el
        }

    /** Maps decoded wire values to something actors accept (`Any`, not `Any?`). JSON `null` → [Unit]. */
    fun asGenServerPayload(decoded: Any?): Any = decoded ?: Unit

    fun decodeGenServerPayload(el: JsonElement): Any = asGenServerPayload(decodePayload(el))
}
