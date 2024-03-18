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