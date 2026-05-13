package org.otpstudy.jinterface

import com.ericsson.otp.erlang.OtpErlangAtom
import com.ericsson.otp.erlang.OtpErlangObject
import com.ericsson.otp.erlang.OtpErlangPid
import com.ericsson.otp.erlang.OtpErlangTuple
import com.ericsson.otp.erlang.OtpNode
import com.ericsson.otp.erlang.OtpMbox
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.InfoMsg

/**
 * A kotlin-otp actor node that is a real participant in an Erlang cluster.
 *
 * Uses the official `jinterface` library (`org.erlang.otp:jinterface`) which implements the
 * Erlang distribution protocol in pure Java. Bundled with every OTP installation since R9.
 *
 * ### Expose a local actor to Erlang:
 * ```kotlin
 * val bridge = OtpErlangBridge("myapp@localhost", "secret", scope)
 * bridge.expose("counter", counterRef, CounterCodec)
 * // Erlang: gen_server:call({'counter', 'myapp@localhost'}, {increment, 1})
 * ```
 *
 * ### Call a real Erlang process from Kotlin:
 * ```kotlin
 * val remote = bridge.remoteRef("erlang_server", "remote@host", MyCodec)
 * val result = remote.call(MyRequest(...))
 * ```
 *
 * OTP source: `lib/jinterface/java_src/com/ericsson/otp/erlang/OtpNode.java`
 */
class OtpErlangBridge(
    val nodeName: String,
    val cookie: String,
    private val scope: CoroutineScope,
) {
    private val node = OtpNode(nodeName, cookie)

    /**
     * Expose [ref] as a registered process named [erlangName] on this node.
     *
     * Incoming `gen_server:call` messages are decoded via [codec], dispatched to [ref.call],
     * and the reply is sent back as an Erlang term.
     * Incoming `gen_server:cast` messages are dispatched to [ref.cast].
     * DOWN messages are forwarded as [ErlangDownMsg] via [ref.sendInfo].
     */
    fun <Req : Any, Rep : Any> expose(
        erlangName: String,
        ref: GenServerRef<*>,
        codec: ErlangCodec<Req, Rep>,
    ) {
        val mbox = node.createMbox(erlangName)
        scope.launch {
            while (isActive) {
                val msg = mbox.receive() ?: break
                handleErlangMessage(msg, ref, codec, mbox)
            }
        }
    }

    /**
     * Return a proxy that sends messages to a real Erlang process on [targetNode].
     * Calls are encoded as `gen_server:call` tuples, sent over Erlang distribution,
     * and replies decoded back.
     */
    fun <Req : Any, Rep : Any> remoteRef(
        erlangName: String,
        targetNode: String,
        codec: ErlangCodec<Req, Rep>,
    ): ErlangRemoteRef<Req, Rep> = ErlangRemoteRef(node, erlangName, targetNode, codec)

    fun shutdown() { node.close() }

    @Suppress("UNCHECKED_CAST")
    private suspend fun <Req : Any, Rep : Any> handleErlangMessage(
        msg: OtpErlangObject,
        ref: GenServerRef<*>,
        codec: ErlangCodec<Req, Rep>,
        mbox: OtpMbox,
    ) {
        if (msg !is OtpErlangTuple) return
        val elems = msg.elements()
        if (elems.isEmpty()) return
        val tag = elems[0]
        when {
            // {'$gen_call', {CallerPid, Ref}, Payload}
            tag is OtpErlangAtom && tag.atomValue() == "\$gen_call" && elems.size == 3 -> {
                val fromTuple = elems[1] as? OtpErlangTuple ?: return
                val callerPid = fromTuple.elementAt(0) as? OtpErlangPid ?: return
                val callRef = fromTuple.elementAt(1)
                val request = codec.decodeRequest(elems[2])
                val reply = (ref as GenServerRef<Any>).call<Rep>(request)
                mbox.send(callerPid, OtpErlangTuple(arrayOf(callRef, codec.encodeReply(reply))))
            }
            // {'$gen_cast', Payload}
            tag is OtpErlangAtom && tag.atomValue() == "\$gen_cast" && elems.size == 2 -> {
                val request = codec.decodeRequest(elems[1])
                ref.cast(request)
            }
            // {'DOWN', Ref, process, Pid, Reason}
            tag is OtpErlangAtom && tag.atomValue() == "DOWN" && elems.size == 5 -> {
                ref.sendInfo(ErlangDownMsg(elems[1], elems[3], elems[4]))
            }
        }
    }
}

/**
 * An [InfoMsg] representing an Erlang DOWN signal delivered via `jinterface`.
 *
 * Analogous to OTP's `{'DOWN', Ref, process, Pid, Reason}` monitor notification.
 */
data class ErlangDownMsg(
    val ref: OtpErlangObject,
    val pid: OtpErlangObject,
    val reason: OtpErlangObject,
) : InfoMsg

/**
 * Proxy for calling a named Erlang process using the `gen_server:call` wire format.
 */
class ErlangRemoteRef<Req : Any, Rep : Any>(
    private val node: OtpNode,
    private val erlangName: String,
    private val targetNode: String,
    private val codec: ErlangCodec<Req, Rep>,
) {
    private val mbox = node.createMbox()

    suspend fun call(request: Req): Rep {
        val ref = node.createRef()
        val selfPid = mbox.self()
        val payload = codec.encodeRequest(request)
        // Wire format: {'$gen_call', {SelfPid, Ref}, Payload}
        val callMsg = OtpErlangTuple(arrayOf(
            OtpErlangAtom("\$gen_call"),
            OtpErlangTuple(arrayOf(selfPid, ref)),
            payload,
        ))
        mbox.send(erlangName, targetNode, callMsg)
        val reply = mbox.receive()
        require(reply is OtpErlangTuple && reply.arity() == 2) {
            "unexpected reply format from $erlangName@$targetNode: $reply"
        }
        return codec.decodeReply(reply.elementAt(1))
    }
}
