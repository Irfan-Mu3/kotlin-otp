package org.otpstudy.poolboy.example

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import org.otpstudy.genserver.GenServer
import org.otpstudy.genserver.GenServerRef
import org.otpstudy.genserver.GenServers
import org.otpstudy.genserver.InitResult
import org.otpstudy.genserver.NoreplyResult
import org.otpstudy.genserver.ReplyResult
import org.otpstudy.genserver.TerminateReason
import org.otpstudy.poolboy.WorkerFactory

/**
 * Configuration for an [ExampleWorker]. Mirrors the worker args proplist passed to
 * `example_worker:start_link/1` in the
 * [poolboy README example](https://github.com/devinus/poolboy/blob/master/README.md).
 *
 * `epgsql` would receive `{hostname, host}, {database, db}, {username, u}, {password, p}` —
 * for the in-memory H2 port we only need a JDBC URL plus the optional credentials
 * H2 will accept (and effectively ignore for `mem:` URLs).
 */
data class WorkerArgs(
    val jdbcUrl: String,
    val username: String = "sa",
    val password: String = "",
)

/**
 * GenServer protocol mirror of `example_worker.erl`.
 *
 * - [Squery] = `gen_server:call(Worker, {squery, Sql})` -> raw `Statement.execute(sql)`.
 *   Returns [SqlResult.Rows] for SELECTs (or DDL with implicit result set), [SqlResult.Updated]
 *   for DML, [SqlResult.Ok] for DDL.
 * - [Equery] = `gen_server:call(Worker, {equery, Sql, Params})` -> [java.sql.PreparedStatement].
 */
sealed interface WorkerRequest {
    data class Squery(val sql: String) : WorkerRequest
    data class Equery(val sql: String, val params: List<Any?>) : WorkerRequest
}

sealed interface SqlResult {
    data object Ok : SqlResult
    data class Updated(val rowCount: Int) : SqlResult
    data class Rows(val rows: List<List<Any?>>) : SqlResult
}

/**
 * Per-worker state. [conn] is the live JDBC connection (closed in [ExampleWorker.terminate]).
 * Public because it appears in [ExampleWorker]'s [GenServer] type parameter.
 */
data class WorkerState(val args: WorkerArgs, val conn: Connection)

/**
 * Faithful Kotlin port of `example_worker.erl` from the poolboy README. Holds one
 * live JDBC connection in its state; closed in [terminate].
 *
 * JDBC is blocking; this worker is intended to be started on [kotlinx.coroutines.Dispatchers.IO]
 * (or a [java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor]-backed dispatcher
 * on JDK 21+). See [LIMITATIONS.md](../../../../../../../../LIMITATIONS.md) "isolation ladder".
 */
class ExampleWorker(private val args: WorkerArgs) : GenServer<WorkerState> {
    override suspend fun init(self: GenServerRef<WorkerState>): InitResult<WorkerState> {
        val conn = DriverManager.getConnection(args.jdbcUrl, args.username, args.password)
        conn.autoCommit = true
        return InitResult.Ok(WorkerState(args, conn))
    }

    override suspend fun handleCall(
        request: Any,
        state: WorkerState,
    ): ReplyResult<WorkerState> {
        val response: SqlResult =
            when (request) {
                is WorkerRequest.Squery -> runSquery(state, request.sql)
                is WorkerRequest.Equery -> runEquery(state, request.sql, request.params)
                else -> error("unknown request: $request")
            }
        return ReplyResult.Reply(response, state)
    }

    override suspend fun handleCast(
        request: Any,
        state: WorkerState,
    ): NoreplyResult<WorkerState> = NoreplyResult.Noreply(state)

    override suspend fun terminate(reason: TerminateReason, state: WorkerState) {
        try {
            state.conn.close()
        } catch (_: Throwable) {
        }
    }

    private fun runSquery(state: WorkerState, sql: String): SqlResult {
        state.conn.createStatement().use { stmt ->
            val isResultSet = stmt.execute(sql)
            return if (isResultSet) {
                SqlResult.Rows(materialize(stmt.resultSet))
            } else {
                val updateCount = stmt.updateCount
                if (updateCount < 0) SqlResult.Ok else SqlResult.Updated(updateCount)
            }
        }
    }

    private fun runEquery(state: WorkerState, sql: String, params: List<Any?>): SqlResult {
        state.conn.prepareStatement(sql).use { ps ->
            params.forEachIndexed { i, p -> ps.setObject(i + 1, p) }
            val isResultSet = ps.execute()
            return if (isResultSet) {
                SqlResult.Rows(materialize(ps.resultSet))
            } else {
                val updateCount = ps.updateCount
                if (updateCount < 0) SqlResult.Ok else SqlResult.Updated(updateCount)
            }
        }
    }

    private fun materialize(rs: ResultSet): List<List<Any?>> {
        val cols = rs.metaData.columnCount
        val rows = mutableListOf<List<Any?>>()
        while (rs.next()) {
            val row = ArrayList<Any?>(cols)
            for (c in 1..cols) row.add(rs.getObject(c))
            rows.add(row)
        }
        return rows
    }

    companion object {
        /** Convenience: factory that starts a worker bound to [args] under the given scope. */
        fun factory(args: WorkerArgs): WorkerFactory<WorkerState> =
            WorkerFactory { scope -> GenServers.startLink(scope, ExampleWorker(args)) }
    }
}
