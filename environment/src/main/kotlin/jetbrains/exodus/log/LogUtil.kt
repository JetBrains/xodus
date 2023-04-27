/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.log

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.util.IOUtil.listFiles
import java.io.File
import java.io.FilenameFilter
import java.io.IOException
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.Predicate
import java.util.stream.Stream

object LogUtil {
    const val LOG_BLOCK_ALIGNMENT = 1024 // log files are aligned by kilobytes
    private const val LOG_FILE_NAME_LENGTH = 11
    const val LOG_FILE_EXTENSION_LENGTH = 3
    const val LOG_FILE_NAME_WITH_EXT_LENGTH = LOG_FILE_NAME_LENGTH + LOG_FILE_EXTENSION_LENGTH
    const val LOG_FILE_EXTENSION = ".xd"
    const val TMP_TRUNCATION_FILE_EXTENSION = ".tnc"
    private const val TMP_TRUNCATION_FILE_WITH_EXT_LENGTH = LOG_FILE_EXTENSION_LENGTH +
            TMP_TRUNCATION_FILE_EXTENSION.length
    private val LOG_FILE_NAME_FILTER = FilenameFilter { _: File?, name: String ->
        name.length == LOG_FILE_NAME_WITH_EXT_LENGTH &&
                name.endsWith(LOG_FILE_EXTENSION)
    }
    private val TMP_TRUNCATION_FILE_NAME_FILTER = Predicate { path: Path ->
        path.fileName.toString().length == TMP_TRUNCATION_FILE_WITH_EXT_LENGTH &&
                path.endsWith(TMP_TRUNCATION_FILE_EXTENSION)
    }
    private val LOG_METADATA_FILE_NAME_FILTER =
        FilenameFilter { _: File?, name: String -> name == StartupMetadata.FIRST_FILE_NAME || name == StartupMetadata.SECOND_FILE_NAME }
    private val LOG_FILE_EXTENSION_CHARS = LOG_FILE_EXTENSION.toCharArray()
    private val LOG_FILE_NAME_ALPHABET = "0123456789abcdefghijklmnopqrstuv".toCharArray()
    private val ALPHA_INDEXES: IntHashMap<Int> = IntHashMap()

    init {
        val alphabet = LOG_FILE_NAME_ALPHABET
        for (i in alphabet.indices) {
            ALPHA_INDEXES.put(alphabet[i].code, Integer.valueOf(i))
        }
    }

    @JvmStatic
    fun getLogFilename(address: Long): String {
        var input = address
        if (input < 0) {
            throw ExodusException("Starting address of a log file is negative: $input")
        }
        if (input % LOG_BLOCK_ALIGNMENT != 0L) {
            throw ExodusException("Starting address of a log file is badly aligned: $input")
        }
        input /= LOG_BLOCK_ALIGNMENT.toLong()
        val name = CharArray(LOG_FILE_NAME_WITH_EXT_LENGTH)
        for (i in 1..LOG_FILE_NAME_LENGTH) {
            name[LOG_FILE_NAME_LENGTH - i] = LOG_FILE_NAME_ALPHABET[(input and 0x1fL).toInt()]
            input = input shr 5
        }
        System.arraycopy(LOG_FILE_EXTENSION_CHARS, 0, name, LOG_FILE_NAME_LENGTH, LOG_FILE_EXTENSION_LENGTH)
        return String(name)
    }

    @JvmStatic
    fun getAddress(logFilename: String): Long {
        val length = logFilename.length
        if (length != LOG_FILE_NAME_WITH_EXT_LENGTH || !logFilename.endsWith(LOG_FILE_EXTENSION)) {
            throw ExodusException("Invalid log file name: $logFilename")
        }
        var address: Long = 0
        for (i in 0 until LOG_FILE_NAME_LENGTH) {
            val c = logFilename[i]
            val integer = ALPHA_INDEXES[c.code] ?: throw ExodusException("Invalid log file name: $logFilename")
            address = (address shl 5) + integer.toLong()
        }
        return address * LOG_BLOCK_ALIGNMENT
    }

    @Suppress("unused")
    fun isLogFile(file: File): Boolean {
        return isLogFileName(file.name)
    }

    @JvmStatic
    fun isLogFileName(name: String): Boolean {
        return try {
            getAddress(name)
            true
        } catch (e: ExodusException) {
            false
        }
    }

    fun listFiles(directory: File): Array<File> {
        return listFiles(directory, LOG_FILE_NAME_FILTER)
    }

    @Throws(IOException::class)
    fun listTlcFiles(directory: File): Stream<Path> {
        return Files.list(Path.of(directory.toURI())).filter(TMP_TRUNCATION_FILE_NAME_FILTER)
    }

    fun listMetadataFiles(directory: File): Array<File> {
        return listFiles(directory, LOG_METADATA_FILE_NAME_FILTER)
    }

    fun listFileAddresses(directory: File): LongArrayList {
        val files = listFiles(directory)
        val result = LongArrayList(files.size)
        for (file in files) {
            result.add(getAddress(file.name))
        }
        return result
    }

    fun listFileAddresses(fromAddress: Long, directory: File): LongArrayList {
        val files = listFiles(directory)
        val result = LongArrayList()
        for (file in files) {
            val address = getAddress(file.name)
            if (address >= fromAddress) {
                result.add(address)
            }
        }
        return result
    }

    fun getWrongAddressErrorMessage(address: Long, fileLengthBound: Long): String {
        val fileAddress = address - address % fileLengthBound
        return ", address = " + address + ", file = " + getLogFilename(fileAddress)
    }

    fun printStackTrace(stackTraceElements: Array<StackTraceElement>, printWriter: PrintWriter) {
        printWriter.println()
        for (traceElement in stackTraceElements) printWriter.println("\tat $traceElement")
    }
}
