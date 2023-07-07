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

open class DataCorruptionException internal constructor(message: String) : ExodusException(message) {
    private constructor(
        message: String,
        address: Long,
        fileLengthBound: Long
    ) : this(message + LogUtil.getWrongAddressErrorMessage(address, fileLengthBound))

    companion object {
        @JvmStatic
        fun raise(message: String, log: Log, address: Long) {
            checkLogIsClosing(log)
            log.switchToReadOnlyMode()
            throw DataCorruptionException(message, address, log.fileLengthBound)
        }

        fun checkLogIsClosing(log: Log) {
            check(!log.isClosing) { "Attempt to read closed log" }
        }
    }
}