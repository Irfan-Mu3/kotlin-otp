package org.otpstudy.poolboy

import org.otpstudy.distribution.NodeId
import org.otpstudy.registry.ProcessResolver

/**
 * How to reach a pool — mirror of poolboy's `pool()` type
 * ([`poolboy.erl`](https://github.com/devinus/poolboy/blob/master/src/poolboy.erl)).
 */
sealed interface PoolAddress {
  /** Local registered name (`Name` or `{local, Name}` on BEAM) — [org.otpstudy.registry.GlobalProcessRegistry]. */
  data class Local(val name: String) : PoolAddress

  /** `{Name, Node}` — pool registered on [node]'s [org.otpstudy.distribution.LocalNode]. */
  data class OnNode(val name: String, val node: NodeId) : PoolAddress

  /** `{global, Name}` — [org.otpstudy.global.GlobalRegistry]. */
  data class Global(val name: String) : PoolAddress

  /** `{via, Registry, Name}` — pluggable [ProcessResolver]. */
  data class Via(val registry: ProcessResolver, val name: String) : PoolAddress
}
