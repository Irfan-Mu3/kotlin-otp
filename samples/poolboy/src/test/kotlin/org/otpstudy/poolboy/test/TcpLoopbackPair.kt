package org.otpstudy.poolboy.test

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.job
import org.otpstudy.distribution.KotlinNodeTransport
import org.otpstudy.distribution.LocalNode
import org.otpstudy.distribution.NodeId
import org.otpstudy.registry.ProcessRegistry

/**
 * Two [KotlinNodeTransport] instances connected on loopback for multi-node poolboy tests.
 */
class TcpLoopbackPair(
    suffix: String = System.nanoTime().toString(),
    val nodeIdA: NodeId = NodeId("a-$suffix", "loopback"),
    val nodeIdB: NodeId = NodeId("b-$suffix", "loopback"),
) : AutoCloseable {
    val regA = ProcessRegistry()
    val regB = ProcessRegistry()
    val transportA = KotlinNodeTransport(nodeIdA, "", 0)
    val transportB = KotlinNodeTransport(nodeIdB, "", 0)
    val localA = LocalNode(nodeIdA)
    val localB = LocalNode(nodeIdB)

    suspend fun start(scope: CoroutineScope) {
        transportB.startAccepting(scope, regB)
        transportA.startAccepting(scope, regA)
    }

    suspend fun connect(scope: CoroutineScope) {
        transportA.connectOut(scope, regA, nodeIdB, "127.0.0.1", transportB.boundPort)
    }

    fun cancelChildren(scope: CoroutineScope) {
        scope.coroutineContext.job.cancelChildren()
    }

    override fun close() {
        transportA.close()
        transportB.close()
    }
}
