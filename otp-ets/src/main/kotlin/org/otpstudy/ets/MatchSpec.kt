package org.otpstudy.ets

/**
 * Type-safe analogue of ETS match specs (`ets:select/2`, `ets:fun2ms/1`).
 *
 * ETS match specs are a Prolog-like compiled language:
 *   ets:select(Tab, [{ {'$1','$2'}, [{'>', '$2', 5}], ['$1'] }])
 *
 * This Kotlin DSL expresses the same query with compile-time type safety:
 *   val spec = matchSpec<String, Int, String> {
 *       guard { (_, v) -> v > 5 }
 *       project { (k, _) -> k }
 *   }
 *   val keys: List<String> = table.select(spec)
 *
 * OTP source: `lib/stdlib/src/ets.erl` ets:select/2, `lib/stdlib/src/ms_transform.erl`
 */
class MatchSpec<K, V, R>(
    val guard: ((K, V) -> Boolean)?,
    val projection: (K, V) -> R,
)

fun <K, V, R> matchSpec(block: MatchSpecBuilder<K, V, R>.() -> Unit): MatchSpec<K, V, R> =
    MatchSpecBuilder<K, V, R>().also { it.block() }.build()

class MatchSpecBuilder<K, V, R> {
    private var guard: ((K, V) -> Boolean)? = null
    private var projection: ((K, V) -> R)? = null

    fun guard(predicate: (Pair<K, V>) -> Boolean) {
        guard = { k, v -> predicate(k to v) }
    }

    fun project(selector: (Pair<K, V>) -> R) {
        projection = { k, v -> selector(k to v) }
    }

    fun build(): MatchSpec<K, V, R> =
        MatchSpec(guard, checkNotNull(projection) { "matchSpec requires a project { } block" })
}

/** Opaque cursor for continuation-style iteration (mirrors `ets:select/3`). */
data class Continuation<K, V, R>(
    internal val remaining: List<Pair<K, V>>,
    internal val spec: MatchSpec<K, V, R>,
)
