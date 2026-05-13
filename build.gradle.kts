import org.gradle.api.tasks.compile.JavaCompile
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.jetbrains.kotlin.jvm") apply false
}

allprojects {
    group = "org.otpstudy"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

/**
 * Kotlin 2.0.x does not emit JVM 25 bytecode yet; the JDK still runs Gradle as 25.
 * Align Kotlin and Java bytecode to JVM 22 so [compileJava] and [compileKotlin] match.
 */
subprojects {
    tasks.withType<KotlinCompile>().configureEach {
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_22)
        }
    }
    tasks.withType<JavaCompile>().configureEach {
        options.release.set(22)
    }
}
