plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(project(":otp-core"))
    implementation(project(":otp-mailbox"))
    implementation(project(":otp-gen-server"))
    implementation(project(":otp-supervisor"))
    implementation(project(":otp-distribution"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
}

application {
    mainClass.set("org.otpstudy.investigation.InvestigationBenchmarksKt")
}
