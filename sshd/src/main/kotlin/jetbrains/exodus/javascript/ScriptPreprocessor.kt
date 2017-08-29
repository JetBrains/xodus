/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.javascript

object ScriptPreprocessor {

    fun preprocess(cmd: () -> String): String {
        var result = cmd().trim()
        // remove successive spaces
        while (true) {
            val replaced = result.replace("  ", " ")
            if (replaced.length == result.length) break
            result = replaced
        }
        // apply replacement rules
        replacementRules.forEach {
            val pattern = it.first
            if (pattern.endsWith(' ')) {
                if (result.startsWith(pattern, ignoreCase = true)) {
                    it.second.forEach { result = it(result) }
                }
            } else if (result.equals(pattern, ignoreCase = true)) {
                it.second.forEach { result = it(result) }
            }
        }
        return result
    }

    private val surroundWithBrackets: (String) -> String = {
        buildString {
            val spaceIdx = it.indexOf(' ')
            append(it, 0, spaceIdx)
            append('(')
            append(it, spaceIdx + 1, it.length)
            append(')')
        }
    }

    private val spacesToCommas: (String) -> String = { it.replace(' ', ',') }

    private val insertQuotes: (String) -> String = {
        if (it.indexOfAny(charArrayOf('"', '\'')) >= 0) {
            it
        } else {
            buildString {
                val idx = it.indexOf('(') + 1
                append(it, 0, idx)
                if (it[idx] != '"') {
                    append('"')
                }
                append(it, idx, it.length - 1)
                if (it[it.length - 2] != '"') {
                    append('"')
                }
                append(it, it.length - 1, it.length)
            }
        }
    }

    private val replacementRules: List<Pair<String, Array<(String) -> String>>> = arrayListOf(
            "?" to arrayOf({ cmd -> "help()" }),
            "/?" to arrayOf({ cmd -> "help()" }),
            "help" to arrayOf({ cmd -> "help()" }),
            "print " to arrayOf(surroundWithBrackets),
            "println " to arrayOf(surroundWithBrackets),
            "load " to arrayOf(surroundWithBrackets, insertQuotes),
            "gc " to arrayOf(surroundWithBrackets),
            "gc(on)" to arrayOf({ cmd -> "gc(true)" }),
            "gc(off)" to arrayOf({ cmd -> "gc(false)" }),
            "gc" to arrayOf({ cmd -> "gc()" }),
            "open " to arrayOf(surroundWithBrackets, spacesToCommas),
            "get " to arrayOf(surroundWithBrackets, spacesToCommas),
            "put " to arrayOf(surroundWithBrackets, spacesToCommas),
            "delete " to arrayOf(surroundWithBrackets, spacesToCommas),
            "all " to arrayOf(surroundWithBrackets, insertQuotes),
            "all" to arrayOf({ cmd -> "all()" }),
            "find " to arrayOf(surroundWithBrackets, spacesToCommas),
            "findStartingWith " to arrayOf(surroundWithBrackets, spacesToCommas),
            "create " to arrayOf(surroundWithBrackets, spacesToCommas),
            "entity " to arrayOf(surroundWithBrackets, insertQuotes)
    )
}