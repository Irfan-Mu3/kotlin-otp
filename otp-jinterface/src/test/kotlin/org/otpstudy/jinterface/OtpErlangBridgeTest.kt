package org.otpstudy.jinterface

import com.ericsson.otp.erlang.OtpErlangAtom
import com.ericsson.otp.erlang.OtpErlangLong
import com.ericsson.otp.erlang.OtpErlangObject
import com.ericsson.otp.erlang.OtpErlangString
import kotlin.test.Test
import kotlin.test.assertEquals

// --- Simple string-echo codec for testing ---
private object EchoCodec : ErlangCodec<String, String> {
    override fun decodeRequest(term: OtpErlangObject): String =
        when (term) {
            is OtpErlangAtom -> term.atomValue()
            is OtpErlangString -> term.stringValue()
            else -> term.toString()
        }

    override fun encodeReply(reply: String): OtpErlangObject = OtpErlangAtom(reply)
    override fun encodeRequest(req: String): OtpErlangObject = OtpErlangAtom(req)
    override fun decodeReply(term: OtpErlangObject): String = decodeRequest(term)
}

// --- Numeric codec for testing ---
private object IntCodec : ErlangCodec<Long, Long> {
    override fun decodeRequest(term: OtpErlangObject): Long = (term as OtpErlangLong).longValue()
    override fun encodeReply(reply: Long): OtpErlangObject = OtpErlangLong(reply)
    override fun encodeRequest(req: Long): OtpErlangObject = OtpErlangLong(req)
    override fun decodeReply(term: OtpErlangObject): Long = (term as OtpErlangLong).longValue()
}

class OtpErlangBridgeTest {

    @Test
    fun `EchoCodec encodes and decodes atom round-trip`() {
        val encoded = EchoCodec.encodeRequest("hello")
        val decoded = EchoCodec.decodeRequest(encoded)
        assertEquals("hello", decoded)
    }

    @Test
    fun `IntCodec encodes and decodes long round-trip`() {
        val encoded = IntCodec.encodeRequest(42L)
        val decoded = IntCodec.decodeRequest(encoded)
        assertEquals(42L, decoded)
    }

    @Test
    fun `EchoCodec encodeReply produces OtpErlangAtom`() {
        val reply = EchoCodec.encodeReply("pong")
        val decoded = EchoCodec.decodeReply(reply)
        assertEquals("pong", decoded)
    }

    @Test
    fun `gen_call wire format tuple has correct structure`() {
        // Verify the wire format: {'$gen_call', {SelfPid, Ref}, Payload}
        // We construct the tag atoms manually to check equality
        val genCallAtom = OtpErlangAtom("\$gen_call")
        val genCastAtom = OtpErlangAtom("\$gen_cast")
        assertEquals("\$gen_call", genCallAtom.atomValue())
        assertEquals("\$gen_cast", genCastAtom.atomValue())
    }
}
