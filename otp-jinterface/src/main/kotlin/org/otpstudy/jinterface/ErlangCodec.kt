package org.otpstudy.jinterface

import com.ericsson.otp.erlang.OtpErlangObject

/**
 * Bidirectional codec between Kotlin types and Erlang terms.
 *
 * Implement this to expose a [org.otpstudy.genserver.GenServer] via [OtpErlangBridge.expose],
 * or to call a real Erlang process via [OtpErlangBridge.remoteRef].
 *
 * Erlang terms use [com.ericsson.otp.erlang.OtpErlangObject] as their base type:
 * - Atoms: OtpErlangAtom
 * - Tuples: OtpErlangTuple
 * - Lists: OtpErlangList
 * - Integers: OtpErlangLong
 * - Binaries: OtpErlangBinary
 * - PIDs: OtpErlangPid
 * - References: OtpErlangRef
 *
 * OTP source: `lib/jinterface/java_src/com/ericsson/otp/erlang/OtpErlangObject.java`
 */
interface ErlangCodec<Req, Rep> {
    fun decodeRequest(term: OtpErlangObject): Req
    fun encodeReply(reply: Rep): OtpErlangObject
    fun encodeRequest(req: Req): OtpErlangObject
    fun decodeReply(term: OtpErlangObject): Rep
}
