package org.otpstudy.jobs.example

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.otpstudy.jobs.Jobs
import org.otpstudy.jobs.JobsConfig
import org.otpstudy.jobs.QueueOptions
import org.otpstudy.jobs.QueueSpec

fun main() = runBlocking {
    Jobs.startLink(
        this,
        JobsConfig(
            queues =
                listOf(
                    QueueSpec("q", QueueOptions.standardRate(limitPerSecond = 10)),
                ),
            defaultQueue = "q",
        ),
    )
    coroutineScope {
        repeat(5) { i ->
            launch {
                Jobs.run("q") {
                    println("job $i at ${JobsTimestamp.now()}")
                    delay(10)
                }
            }
        }
    }
    delay(500)
    println("approved=${Jobs.queueInfo("q")?.approved}")
    Jobs.ref().stop()
}

private object JobsTimestamp {
    fun now(): Long = org.otpstudy.jobs.JobsTimestamp.now()
}
