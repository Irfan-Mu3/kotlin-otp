import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "2.2.10"
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    compileOnly("org.jetbrains.kotlin:kotlin-compiler-embeddable:2.2.10")
    implementation(kotlin("gradle-plugin"))
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_22)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(22)
}
