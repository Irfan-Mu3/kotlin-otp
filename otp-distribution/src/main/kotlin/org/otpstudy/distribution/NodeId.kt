package org.otpstudy.distribution

/**
 * Identifies an actor runtime node. Analogous to an Erlang node name (`name@host`).
 *
 * All [org.otpstudy.genserver.GenServerRef]s are addressable on a node; [LocalNode] is
 * the single-JVM case. [RemoteNodeStub] routes to another node via a [NodeTransport].
 *
 * OTP source: `lib/kernel/src/net_kernel.erl`, `erlang:node/0`
 */
data class NodeId(val name: String, val host: String = "local") {
    override fun toString() = "$name@$host"

    companion object {
        val LOCAL = NodeId("local")
    }
}
