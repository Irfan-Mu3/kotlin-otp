import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    kotlin("jvm")
    id("org.jetbrains.kotlin.plugin.serialization")
}

dependencies {
    api(project(":otp-core"))
    api(project(":otp-gen-server"))
    api(project(":otp-supervisor"))
    api(project(":otp-registry"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.1")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
    // -D after the task list is unreliable on the Gradle JVM; also accept -P for convenience.
    val otpDebug =
        System.getProperty("org.otpstudy.debug")
            ?: project.findProperty("org.otpstudy.debug")?.toString()
    otpDebug?.let { systemProperty("org.otpstudy.debug", it) }
    val debugOn = otpDebug?.equals("true", ignoreCase = true) == true
    if (debugOn) {
        // Otherwise [OtpStudyDebug] writes to the worker's stderr and Gradle prints nothing → empty pipes.
        testLogging {
            events(TestLogEvent.STANDARD_OUT, TestLogEvent.STANDARD_ERROR)
            showStandardStreams = true
        }
    }
    // Pass -PskipSlowTests=true (or set org.otpstudy.skipSlowTests=true) to skip the
    // multi-process wire soak tests that spawn external JVMs and take ~2 minutes each.
    val skipSlow =
        System.getProperty("org.otpstudy.skipSlowTests")?.equals("true", ignoreCase = true)
            ?: project.findProperty("skipSlowTests")?.toString()?.equals("true", ignoreCase = true)
            ?: false
    if (skipSlow) {
        exclude("**/KotlinNodeTransportMultiProcessTest*")
        exclude("**/KotlinNodeTransportLoopbackTest*")
        exclude("**/DistributionWireTest*")
    }
}
