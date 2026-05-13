plugins {
    kotlin("jvm")
}

dependencies {
    api(project(":otp-gen-server"))
    api("org.erlang.otp:jinterface:1.6.1")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:5.9.1")
    testImplementation("io.kotest:kotest-property:5.9.1")
}

tasks.test {
    useJUnitPlatform()
}
