
/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.query.metadata

import mu.KLogger

interface PaddedLogger {
    companion object {
        //todo switch to configuration loading
        private val VERBOSE_LOGGING get() = System.getProperty("jetbrains.xodus.oxigendb.verbose_schema_logging", "false").toBoolean()
        fun logger(logger: KLogger): PaddedLogger = if (VERBOSE_LOGGING) PaddedLoggerImpl(logger) else DisabledPaddedLogger()
    }
    fun append(s: String)
    fun appendLine(s: String)
    fun updatePadding(paddingShift: Int)
    fun flush()
}

class PaddedLoggerImpl(
    private val logger: KLogger
) : PaddedLogger {
    private var paddingCount: Int = 0
    private val sb = StringBuilder()

    private var newLine: Boolean = false

    override fun append(s: String) {
        addPaddingIfNewLine()
        sb.append(s)
    }

    override fun appendLine(s: String) {
        addPaddingIfNewLine()
        sb.appendLine(s)
        newLine = true
    }

    override fun updatePadding(paddingShift: Int) {
        paddingCount += paddingShift
    }

    override fun flush() {
        // trim last \n
        if (sb.isNotEmpty() && sb.last() == '\n') {
            sb.setLength(sb.length - 1)
        }
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

class DisabledPaddedLogger : PaddedLogger {
    override fun append(s: String) = Unit

    override fun appendLine(s: String) = Unit

    override fun updatePadding(paddingShift: Int) = Unit

    override fun flush() = Unit
}
