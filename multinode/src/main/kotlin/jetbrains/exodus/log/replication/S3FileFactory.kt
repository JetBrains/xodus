/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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

import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogUtil
import mu.KLogging
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.nio.file.Path

class S3FileFactory(
        override val s3: S3AsyncClient,
        val dir: Path,
        override val bucket: String,
        override val requestOverrideConfig: AwsRequestOverrideConfig? = null
) : S3FactoryBoilerplate, FileFactory {
    companion object : KLogging()

    override fun fetchFile(log: Log, address: Long, startingLength: Long, expectedLength: Long, finalFile: Boolean): WriteResult {
        if (checkPreconditions(log, expectedLength, startingLength)) return WriteResult.empty

        log.ensureWriter().fileSetMutable.add(address)

        val filename = LogUtil.getLogFilename(address)

        logger.debug { "Fetch file at $filename" }

        val file = dir.resolve(filename)
        val handler = if (finalFile) {
            // this is intentional, aligns last page within file
            val lastPageStart = log.getHighPageAddress(expectedLength)
            FileAsyncHandler(
                    file,
                    startingLength,
                    lastPageStart,
                    log.ensureWriter().allocLastPage(address + lastPageStart)
            )
        } else {
            FileAsyncHandler(file, startingLength)
        }

        return getRemoteFile(expectedLength, startingLength, filename, handler).get().also {
            log.ensureWriter().apply {
                incHighAddress(it.written)
                lastPageLength = it.lastPageLength
            }
        }
    }
}
