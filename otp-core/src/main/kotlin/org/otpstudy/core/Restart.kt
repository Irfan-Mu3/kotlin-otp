package org.otpstudy.core

/**
 * Mirrors OTP [restart](https://www.erlang.org/doc/man/supervisor.html#child_spec-restart) in child specs.
 */
enum class Restart {
    /** Always restart on abnormal termination. */
    Permanent,

    /** Never restart. */
    Temporary,

    /** Restart only on abnormal exit (failure), not on normal completion. */
    Transient,
}
