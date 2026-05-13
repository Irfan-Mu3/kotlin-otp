rootProject.name = "kotlin-otp"

pluginManagement {
    plugins {
        id("org.jetbrains.kotlin.jvm") version "2.2.10"
        id("org.jetbrains.kotlin.plugin.serialization") version "2.2.10"
    }
}

include(
    "otp-core",
    "otp-mailbox",
    "otp-gen-server",
    "otp-gen-statem",
    "otp-supervisor",
    "otp-registry",
    "otp-application",
    "otp-gen-event",
    "otp-distribution",
    "otp-ets",
    "otp-observer",
    "otp-memory",
    "otp-hotcode",
    "otp-jinterface",
    "otp-pg",
    "otp-global",
    "otp-mnesia",
    "otp-logger",
    "otp-sasl",
    "otp-recon",
    "otp-trace",
    "otp-dets",
    "samples:demo",
    "samples:poolboy",
)
