/**
 * Copyright 2010 - 2020 JetBrains s.r.o.
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
package jetbrains.exodus.log.replication

import jetbrains.exodus.ExodusException
import jetbrains.exodus.log.LogUtil

internal const val logFileNameWithExtLength = 19

internal fun String.toFileName() = this + LogUtil.LOG_FILE_EXTENSION

internal val String.address get() = LogUtil.getAddress(this)

internal fun decodeAddress(logFilename: String): Long {
    if (!checkAddress(logFilename)) {
        throw ExodusException("Invalid log file name: $logFilename")
    }
    return logFilename.substring(0, 16).toLong(16)
}

internal fun checkAddress(logFilename: String): Boolean {
    return logFilename.length == logFileNameWithExtLength && logFilename.endsWith(LogUtil.LOG_FILE_EXTENSION)
}

internal val String.isValidAddress
    get() =
        this.length == LogUtil.LOG_FILE_NAME_WITH_EXT_LENGTH && LogUtil.isLogFileName(this)

internal val String.isValidSubFolder: Boolean
    get() {
        val paths = this.split("/")
        val isValidFolderName = paths[0].let {
            it.startsWith("_") && it.drop(1).toFileName().isValidAddress
        }
        return isValidFolderName && paths.size == 2 && checkAddress(paths[1])
    }