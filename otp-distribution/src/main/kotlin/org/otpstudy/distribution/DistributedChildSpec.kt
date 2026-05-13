package org.otpstudy.distribution

import org.otpstudy.supervisor.ChildSpec

/**
 * A child spec annotated with a preferred node and ordered fallbacks.
 *
 * Used by a `DistributedSupervisor` (not yet implemented): when [preferredNode] becomes
 * unreachable, the supervisor starts the child on the first reachable [fallbackNodes] entry.
 *
 * Analogous to OTP's `pg` / global process groups and distributed supervisor patterns.
 */
data class DistributedChildSpec(
    val spec: ChildSpec,
    val preferredNode: NodeId,
    val fallbackNodes: List<NodeId> = emptyList(),
)
