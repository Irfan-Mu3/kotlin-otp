package org.otpstudy.poolboy.example

import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.otpstudy.application.ApplicationEnv
import org.otpstudy.application.ApplicationStartResult
import org.otpstudy.application.SupervisorApplication
import org.otpstudy.core.Restart
import org.otpstudy.core.Shutdown
import org.otpstudy.poolboy.PoolConfig
import org.otpstudy.poolboy.PoolRef
import org.otpstudy.poolboy.Poolboy
import org.otpstudy.poolboy.transaction
import org.otpstudy.supervisor.ChildSpec
import org.otpstudy.supervisor.SupervisorFlags

/**
 * One pool's worth of config, parsed from [ApplicationEnv]. Mirrors a single tuple of
 * the `pools` proplist in the README's `example.app`:
 *
 * ```erlang
 * {pool1, [{size, 10}, {max_overflow, 20}],
 *  [{hostname, "127.0.0.1"}, {database, "db1"}, {username, "db1"}, {password, "abc123"}]}
 * ```
 */
data class PoolDef(
    val name: String,
    val poolConfig: PoolConfig,
    val workerArgs: WorkerArgs,
)

/**
 * Faithful Kotlin port of `example.erl` + `example.app` from the poolboy README.
 *
 * Each pool becomes one [ChildSpec] under a single [SupervisorApplication]; the child's
 * `start` lambda calls [Poolboy.startLink] and joins on the pool gen-server's job (so the
 * supervisor's "restart on abnormal exit" semantics apply: if a pool itself crashes, the
 * application supervisor will restart it via the [PoolDef] factory).
 *
 * Exposes per-pool [CompletableDeferred] handles so callers can borrow a worker once
 * the supervisor finishes starting (analogue of `application:start(example), poolboy:transaction(pool1, ...)`
 * in the README).
 */
class ExampleApp(env: ApplicationEnv) {
    @Suppress("UNCHECKED_CAST")
    private val pools: List<PoolDef> =
        env.require<List<PoolDef>>("pools")

    val handles: Map<String, CompletableDeferred<PoolRef<WorkerState>>> =
        pools.associate { it.name to CompletableDeferred() }

    private val children: List<ChildSpec> =
        pools.map { def ->
            val ready = handles.getValue(def.name)
            ChildSpec(
                id = def.name,
                restart = Restart.Permanent,
                shutdown = Shutdown.Timeout(5.seconds),
                start = {
                    val pool = Poolboy.startLink(this, def.poolConfig, ExampleWorker.factory(def.workerArgs))
                    ready.complete(pool)
                    pool.ref.job.join()
                },
            )
        }

    val application: SupervisorApplication =
        SupervisorApplication(
            name = "example",
            flags =
                SupervisorFlags(
                    intensity = 10,
                    period = 60.seconds,
                ),
            children = children,
        )
}

/**
 * Build a default two-pool [ApplicationEnv] mirroring the README example. Both pools
 * are in-memory H2 with `DB_CLOSE_DELAY=-1` so they survive worker restarts.
 *
 * `pool1`: size 10, maxOverflow 20.
 * `pool2`: size 5, maxOverflow 10.
 */
fun defaultExampleEnv(): ApplicationEnv =
    ApplicationEnv(
        mapOf(
            "pools" to
                listOf(
                    PoolDef(
                        name = "pool1",
                        poolConfig = PoolConfig(size = 10, maxOverflow = 20, name = "pool1"),
                        workerArgs = WorkerArgs(jdbcUrl = "jdbc:h2:mem:pool1;DB_CLOSE_DELAY=-1"),
                    ),
                    PoolDef(
                        name = "pool2",
                        poolConfig = PoolConfig(size = 5, maxOverflow = 10, name = "pool2"),
                        workerArgs = WorkerArgs(jdbcUrl = "jdbc:h2:mem:pool2;DB_CLOSE_DELAY=-1"),
                    ),
                ),
        ),
    )

/**
 * `main` entry point — mirrors `example:start()` followed by an interactive checkout.
 *
 * Demonstrates: start the application, do a CREATE/INSERT/SELECT cycle through `pool1`,
 * stop cleanly. Run with `./gradlew :samples:poolboy:run`.
 */
fun main(): Unit =
    runBlocking {
        val app = ExampleApp(defaultExampleEnv())
        when (val started = app.application.start(this)) {
            is ApplicationStartResult.Error -> {
                println("application start failed: ${started.cause.message}")
                return@runBlocking
            }
            is ApplicationStartResult.PhaseError -> {
                println("phase '${started.phase}' failed: ${started.cause.message}")
                return@runBlocking
            }
            is ApplicationStartResult.Ok -> {
                val pool1 = app.handles.getValue("pool1").await()
                println("pool1 status (initial): ${pool1.status()}")

                pool1.transaction { worker ->
                    worker.call<SqlResult>(WorkerRequest.Squery("CREATE TABLE IF NOT EXISTS t(id INT)"))
                    worker.call<SqlResult>(WorkerRequest.Squery("INSERT INTO t VALUES (1), (2), (3)"))
                    val rows = worker.call<SqlResult>(WorkerRequest.Squery("SELECT COUNT(*) FROM t"))
                    println("rows in t: $rows")
                }

                println("pool1 status (after txn): ${pool1.status()}")
                app.application.stop()
                println("application stopped")
            }
        }
    }
