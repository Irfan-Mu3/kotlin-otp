package org.otpstudy.distribution

import org.otpstudy.genserver.GenServerRef
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.longOrNull

/**
 * JSON payloads on the Kotlin dist wire. Primitives and [JsonElement] are supported; for structured
 * application data prefer building a [JsonObject] / [JsonArray] (or encode to string) at the edge.
 */
internal object DistributionWire {
    val json = Json {
        classDiscriminator = "type"
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

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
            else -> JsonPrimitive(value.toString())
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

    /**
     * Maps decoded wire values to something [GenServerRef.cast] / [call] accept (`Any`, not `Any?`).
     * JSON `null` becomes [Unit].
     */
    fun asGenServerPayload(decoded: Any?): Any = decoded ?: Unit

    /** [decodePayload] then [asGenServerPayload] — single entry for inbound frames to actors. */
    fun decodeGenServerPayload(el: JsonElement): Any = asGenServerPayload(decodePayload(el))
}
