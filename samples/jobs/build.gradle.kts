plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(project(":otp-application"))
    implementation(project(":otp-gen-server"))
    implementation(project(":otp-core"))
    implementation(project(":otp-registry"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

    testImplementation(kotlin("test"))
    testImplementation(project(":otp-testkit"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

application {
    mainClass.set("org.otpstudy.jobs.example.JobsExampleKt")
}

tasks.test {
    useJUnitPlatform()
}
