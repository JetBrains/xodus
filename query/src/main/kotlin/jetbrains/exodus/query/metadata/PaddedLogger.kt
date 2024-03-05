package jetbrains.exodus.query.metadata

import mu.KLogger

class PaddedLogger(
    private val logger: KLogger
) {
    private var paddingCount: Int = 0
    private val sb = StringBuilder()

    private var newLine: Boolean = false

    fun append(s: String) {
        addPaddingIfNewLine()
        sb.append(s)
    }

    fun appendLine(s: String = "") {
        addPaddingIfNewLine()
        sb.appendLine(s)
        newLine = true
    }

    fun updatePadding(paddingShift: Int) {
        paddingCount += paddingShift
    }

    fun flush() {
        logger.info { sb.toString() }
        sb.clear()
        newLine = true
        paddingCount = 0
    }

    private fun addPaddingIfNewLine() {
        if (newLine) {
            sb.append(" ".repeat(paddingCount))
            newLine = false
        }
    }
}

fun PaddedLogger.withPadding(padding: Int = 4, code: () -> Unit) {
    updatePadding(padding)
    code()
    updatePadding(-padding)
}