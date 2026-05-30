plugins {
    kotlin("jvm")
    application
}

dependencies {
    // Apache Pekko (open-source Akka continuation, typed actors API)
    implementation("org.apache.pekko:pekko-actor-typed_2.13:1.6.0")
    implementation("org.scala-lang:scala-library:2.13.17")

    // Akka typed actors API (for direct Akka comparison)
    implementation("com.typesafe.akka:akka-actor-typed_2.13:2.6.21")

    // Vert.x core (EventBus request/reply, fire-and-forget)
    implementation("io.vertx:vertx-core:5.1.0")

    // Kotlin coroutines (raw Channel baseline — already in project, pinned here explicitly)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
}

application {
    mainClass.set("org.otpstudy.benchmarks.CompetitorBenchmarksKt")
    // Increase stack and heap for warm-up + concurrent-caller scenarios
    applicationDefaultJvmArgs = listOf("-Xms256m", "-Xmx512m")
}
