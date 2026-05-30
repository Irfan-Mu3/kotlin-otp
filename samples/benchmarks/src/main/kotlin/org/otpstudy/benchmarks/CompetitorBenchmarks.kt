package org.otpstudy.benchmarks

import kotlin.math.sqrt
import kotlin.system.measureNanoTime

// ---------------------------------------------------------------------------
// Shared data model (mirrors InvestigationBenchmarks.kt structure)
// ---------------------------------------------------------------------------

data class BenchResult(
    val library: String,
    val scenario: String,
    val iterations: Int,
    val p50Micros: Double,
    val p95Micros: Double,
    val p99Micros: Double = 0.0,
    val p999Micros: Double = 0.0,
    val throughputPerSec: Double,
    val notes: String = "",
)

// ---------------------------------------------------------------------------
// Profile configuration
// ---------------------------------------------------------------------------

data class BenchProfile(
    val name: String,
    val warmupIterations: Int,
    val iterations: Int,
    val concurrentCallerCounts: List<Int>,
    val callsPerCaller: Int,
    val rounds: Int,
    val runOtp: Boolean,
)

fun parseProfile(args: Array<String>): BenchProfile {
    val profileArg = args.firstOrNull { it.startsWith("--profile=") }?.substringAfter("=") ?: "quick"
    val roundsArg = args.firstOrNull { it.startsWith("--rounds=") }?.substringAfter("=")?.toIntOrNull()
    val runOtp = args.any { it == "--otp" }
    val base = when (profileArg.lowercase()) {
        "long" -> BenchProfile(
            name = "long",
            warmupIterations = 5_000,
            iterations = 50_000,
            concurrentCallerCounts = listOf(1, 10, 50, 100),
            callsPerCaller = 2_000,
            rounds = 5,
            runOtp = runOtp,
        )
        else -> BenchProfile(
            name = "quick",
            warmupIterations = 2_000,
            iterations = 20_000,
            concurrentCallerCounts = listOf(1, 10, 50),
            callsPerCaller = 500,
            rounds = 1,
            runOtp = runOtp,
        )
    }
    return if (roundsArg != null && roundsArg > 0) base.copy(rounds = roundsArg) else base
}

// ---------------------------------------------------------------------------
// Statistics helpers
// ---------------------------------------------------------------------------

fun percentile(nanos: List<Long>, pct: Double): Double {
    if (nanos.isEmpty()) return 0.0
    val sorted = nanos.sorted()
    val idx = ((pct / 100.0) * (sorted.size - 1)).toInt().coerceIn(0, sorted.lastIndex)
    return sorted[idx] / 1_000.0
}

fun mean(values: List<Double>): Double = if (values.isEmpty()) 0.0 else values.sum() / values.size

fun stddev(values: List<Double>): Double {
    if (values.size < 2) return 0.0
    val m = mean(values)
    return sqrt(values.sumOf { (it - m) * (it - m) } / (values.size - 1))
}

fun Double.pretty(): String = "%.2f".format(this)

// ---------------------------------------------------------------------------
// OTP escript runner (optional — pass --otp flag)
// ---------------------------------------------------------------------------

/** Invokes the Erlang escript and parses its CSV output into BenchResult rows. */
fun runOtpBenchmark(scriptPath: String): List<BenchResult> {
    val escript = resolveEscriptPath()
    if (escript == null) {
        System.err.println("[otp] escript not found on PATH — skipping OTP benchmarks")
        return emptyList()
    }
    return try {
        val proc = ProcessBuilder(escript, scriptPath)
            .redirectErrorStream(true)
            .start()
        val output = proc.inputStream.bufferedReader().readText()
        val exitCode = proc.waitFor()
        if (exitCode != 0) {
            System.err.println("[otp] escript exited with code $exitCode")
            System.err.println("[otp] output: ${output.take(500)}")
        }
        parseOtpCsv(output)
    } catch (e: Exception) {
        System.err.println("[otp] failed to run escript: ${e.message}")
        emptyList()
    }
}

private fun resolveEscriptPath(): String? {
    // Honour explicit env override first, then search PATH
    val env = System.getenv("ESCRIPT_PATH")
    if (env != null) return env
    for (dir in (System.getenv("PATH") ?: "").split(":")) {
        val f = java.io.File(dir, "escript")
        if (f.canExecute()) return f.absolutePath
    }
    return null
}

/**
 * Expected escript CSV format (same columns as InvestigationBenchmarks.kt):
 *   library,scenario,iterations,p50_us,p95_us,p99_us,p999_us,throughput_ops_sec,notes
 */
private fun parseOtpCsv(raw: String): List<BenchResult> {
    return raw.lines()
        .filter { it.startsWith("otp,") }
        .mapNotNull { line ->
            val cols = line.split(",")
            if (cols.size < 9) return@mapNotNull null
            try {
                BenchResult(
                    library = cols[0].trim(),
                    scenario = cols[1].trim(),
                    iterations = cols[2].trim().toInt(),
                    p50Micros = cols[3].trim().toDouble(),
                    p95Micros = cols[4].trim().toDouble(),
                    p99Micros = cols[5].trim().toDouble(),
                    p999Micros = cols[6].trim().toDouble(),
                    throughputPerSec = cols[7].trim().toDouble(),
                    notes = cols.drop(8).joinToString(",").trim(),
                )
            } catch (_: Exception) { null }
        }
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

fun printHeader() {
    println("library,scenario,iterations,p50_us,p95_us,p99_us,p999_us,throughput_ops_sec,notes")
}

fun BenchResult.toCsvRow(): String =
    "$library,$scenario,$iterations,${p50Micros.pretty()},${p95Micros.pretty()},${p99Micros.pretty()},${p999Micros.pretty()},${throughputPerSec.pretty()},${notes.replace(",", ";")}"

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fun main(args: Array<String>) {
    val profile = parseProfile(args)
    println("profile=${profile.name},rounds=${profile.rounds},iterations=${profile.iterations}")
    printHeader()

    // Locate the OTP escript.
    // Gradle's JavaExec working directory is the module directory (samples/benchmarks/).
    // From there the escript lives at src/main/erlang/otp_bench.erl.
    // For non-Gradle invocations from the project root, try the full relative path.
    val escriptFile = sequenceOf(
        java.io.File("src/main/erlang/otp_bench.erl"),                              // Gradle run (cwd = module dir)
        java.io.File("samples/benchmarks/src/main/erlang/otp_bench.erl"),           // from project root
        java.io.File(object {}.javaClass.protectionDomain.codeSource.location.toURI())
            .parentFile?.parentFile?.parentFile?.let {
                java.io.File(it, "erlang/otp_bench.erl")
            } ?: java.io.File(""),
    ).first { it.exists() || it.path.isEmpty() }.takeIf { it.exists() }

    val allResults = mutableListOf<BenchResult>()

    repeat(profile.rounds) { round ->
        val roundResults = mutableListOf<BenchResult>()

        // --- Raw Kotlin Channel floor ---
        roundResults += runChannelCallRoundtrip(profile)
        roundResults += runChannelCastEnqueue(profile)
        roundResults += runChannelConcurrentCallers(profile)

        // --- Pekko typed actors ---
        roundResults += runPekkaCallRoundtrip(profile)
        roundResults += runPekkaCastEnqueue(profile)
        roundResults += runPekkaConcurrentCallers(profile)
        roundResults += runPekkaSupervisorRestart(profile)

        // --- Vert.x EventBus ---
        roundResults += runVertxCallRoundtrip(profile)
        roundResults += runVertxCastEnqueue(profile)
        roundResults += runVertxConcurrentCallers(profile)

        // --- OTP via escript (optional) ---
        if (profile.runOtp) {
            if (escriptFile != null) {
                roundResults += runOtpBenchmark(escriptFile.absolutePath)
            } else {
                System.err.println("[otp] escript file not found — skipping OTP benchmarks")
            }
        }

        for (r in roundResults) {
            println("${round + 1},${r.toCsvRow()}")
        }
        allResults += roundResults
        System.err.println("round=${round + 1} complete")
    }

    // Summary: mean ± stddev per (library, scenario)
    println()
    println("summary_library,summary_scenario,rounds,mean_p50_us,stddev_p50_us,mean_p95_us,stddev_p95_us,mean_p99_us,stddev_p99_us,mean_throughput_ops_sec,stddev_throughput_ops_sec")
    val grouped = allResults.groupBy { "${it.library}::${it.scenario}" }
    for ((key, rows) in grouped) {
        val (lib, scen) = key.split("::")
        println(
            "$lib,$scen,${rows.size}," +
                "${mean(rows.map { it.p50Micros }).pretty()},${stddev(rows.map { it.p50Micros }).pretty()}," +
                "${mean(rows.map { it.p95Micros }).pretty()},${stddev(rows.map { it.p95Micros }).pretty()}," +
                "${mean(rows.map { it.p99Micros }).pretty()},${stddev(rows.map { it.p99Micros }).pretty()}," +
                "${mean(rows.map { it.throughputPerSec }).pretty()},${stddev(rows.map { it.throughputPerSec }).pretty()}"
        )
    }
}
