package org.otpstudy.registry

import org.otpstudy.genserver.GenServerRef

/**
 * Pluggable name registry — OTP `{via, Module, Name}` escape hatch for kotlin-otp.
 */
interface ViaRegistry : ProcessResolver {
  fun register(name: String, ref: GenServerRef<*>)

  fun unregister(name: String, expected: GenServerRef<*>? = null)
}

/**
 * In-memory [ViaRegistry] for tests and single-JVM apps.
 */
class MapViaRegistry : ViaRegistry {
  private val impl = ProcessRegistry()

  override fun register(name: String, ref: GenServerRef<*>) = impl.register(name, ref)

  override fun unregister(name: String, expected: GenServerRef<*>?) = impl.unregister(name, expected)

  override fun lookup(name: String): GenServerRef<*>? = impl.lookup(name)
}
