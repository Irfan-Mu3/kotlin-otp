package org.otpstudy.distribution

import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.otpstudy.core.OtpStudyDebug

/**
 * When [OtpStudyDebug.enabled], logs test lifecycle to stderr (see [OtpStudyDebug]).
 */
class OtpStudyDebugTestExtension : BeforeEachCallback, AfterEachCallback {
    override fun beforeEach(context: ExtensionContext) {
        if (OtpStudyDebug.enabled) {
            System.err.println("[otpstudy-debug] TEST Starting ${context.displayName}")
        }
    }

    override fun afterEach(context: ExtensionContext) {
        if (OtpStudyDebug.enabled) {
            System.err.println("[otpstudy-debug] TEST Finished ${context.displayName}")
        }
    }
}
