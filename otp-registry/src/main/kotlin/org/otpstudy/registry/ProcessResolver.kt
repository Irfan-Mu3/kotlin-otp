package org.otpstudy.registry

import org.otpstudy.genserver.GenServerRef

/**
 * Resolves a registered process by name. Kotlin analogue of addressing a pool or
 * gen_server via `{via, Module, Name}` without dynamic module loading.
 */
interface ProcessResolver {
  fun lookup(name: String): GenServerRef<*>?

  @Suppress("UNCHECKED_CAST")
  fun <S> whereis(name: String): GenServerRef<S>? = lookup(name) as GenServerRef<S>?
}

/** [ProcessResolver] backed by [GlobalProcessRegistry]. */
object GlobalProcessRegistryResolver : ProcessResolver {
  override fun lookup(name: String): GenServerRef<*>? = GlobalProcessRegistry.lookup(name)
}

/** [ProcessResolver] backed by a [ProcessRegistry] instance. */
class ProcessRegistryResolver(private val registry: ProcessRegistry) : ProcessResolver {
  override fun lookup(name: String): GenServerRef<*>? = registry.lookup(name)
}
