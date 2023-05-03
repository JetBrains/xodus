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
package jetbrains.exodus.io

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import java.io.File

class AsyncFileDataReaderWriterProvider : DataReaderWriterProvider() {
    private var env: EnvironmentImpl? = null
    override fun newReaderWriter(location: String): Pair<DataReader, DataWriter> {
        val reader = FileDataReader(checkDirectory(location))
        var lockId: String? = null
        if (env != null) {
            lockId = env!!.environmentConfig.logLockId
        }
        return Pair(reader, AsyncFileDataWriter(reader, lockId))
    }

    override fun onEnvironmentCreated(environment: Environment) {
        env = environment as EnvironmentImpl
        super.onEnvironmentCreated(environment)
    }

    companion object {
        private fun checkDirectory(location: String): File {
            val directory = File(location)
            if (directory.isFile) {
                throw ExodusException("A directory is required: $directory")
            }
            if (!directory.exists() && !directory.mkdirs()) {
                throw ExodusException("Failed to create directory: $directory")
            }
            return directory
        }
    }
}
