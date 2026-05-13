package org.otpstudy.distribution

import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals

class DistributionWireTest {

    @Test
    fun `decodeGenServerPayload maps JsonNull to Unit`() {
        assertEquals(Unit, DistributionWire.decodeGenServerPayload(JsonNull))
    }

    @Test
    fun `decodeGenServerPayload passes through string`() {
        assertEquals("hi", DistributionWire.decodeGenServerPayload(JsonPrimitive("hi")))
    }

    @Test
    fun `asGenServerPayload maps null to Unit`() {
        assertEquals(Unit, DistributionWire.asGenServerPayload(null))
    }

    @Test
    fun `JsonObject roundtrips through encodePayload and decodePayload`() {
        val obj =
            buildJsonObject {
                put("a", 1)
                put("b", "x")
            }
        assertEquals(obj, DistributionWire.encodePayload(obj))
        assertEquals(obj, DistributionWire.decodePayload(obj))
    }
}
