plugins {
    kotlin("jvm")
}

dependencies {
    api(project(":otp-core"))
    api(project(":otp-gen-server"))
    api(project(":otp-gen-statem"))
    api(project(":otp-supervisor"))
    api(project(":otp-ets"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
