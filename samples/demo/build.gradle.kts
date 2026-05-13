plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(project(":otp-application"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
}

application {
    mainClass.set("org.otpstudy.demo.DemoKt")
}
