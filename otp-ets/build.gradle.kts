plugins {
    kotlin("jvm")
}

dependencies {
    api(project(":otp-core"))
    api(project(":otp-gen-server"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
