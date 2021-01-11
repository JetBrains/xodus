/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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

import jetbrains.exodus.log.LogTip
import jetbrains.exodus.log.LogUtil

interface S3DataReaderOrWriter : S3FactoryBoilerplate {

    val logTip: LogTip?
}

internal fun S3FactoryBoilerplate.listObjectsBuilder() = listObjectsBuilder(bucket, requestOverrideConfig)

internal fun getPartialFileName(address: Long): String {
    return String.format("%016x${LogUtil.LOG_FILE_EXTENSION}", address)
}

internal fun getPartialFolderPrefix(blockAddress: Long): String {
    return "_${LogUtil.getLogFilename(blockAddress).replace(LogUtil.LOG_FILE_EXTENSION, "")}/"
}
