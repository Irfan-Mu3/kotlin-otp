plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(project(":otp-application"))
    implementation(project(":otp-supervisor"))
    implementation(project(":otp-gen-server"))
    implementation(project(":otp-core"))
    implementation(project(":otp-registry"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("com.h2database:h2:2.3.232")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

application {
    mainClass.set("org.otpstudy.poolboy.example.ExampleAppKt")
}

tasks.test {
    useJUnitPlatform()
}
