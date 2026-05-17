package org.otpstudy.poolboy

import org.otpstudy.distribution.LocalNode
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.global.GlobalRegistry
import org.otpstudy.registry.ProcessResolver

/** [ProcessResolver] backed by [org.otpstudy.global.GlobalRegistry]. */
object GlobalRegistryResolver : ProcessResolver {
  override fun lookup(name: String): GenServerRef<*>? =
    when (val r = GlobalRegistry.resolveName(name)) {
      is GlobalRegistry.NameResolution.LocalRef<*> -> r.ref
      else -> null
    }
}

/** [ProcessResolver] backed by a [LocalNode] registry. */
class LocalNodeResolver(private val node: LocalNode) : ProcessResolver {
  override fun lookup(name: String): GenServerRef<*>? = node.whereis<Any>(name)
}
