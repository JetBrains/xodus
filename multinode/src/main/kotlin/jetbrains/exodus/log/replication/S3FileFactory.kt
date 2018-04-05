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
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.nio.file.Path

class S3FileFactory(
        override val s3: S3AsyncClient,
        val dir: Path,
        override val bucket: String,
        override val requestOverrideConfig: AwsRequestOverrideConfig? = null
) : S3FactoryBoilerplate {

    override fun fetchFile(log: Log, address: Long, expectedLength: Long, useLastPage: Boolean): WriteResult {
        if (checkPreconditions(log, expectedLength)) return WriteResult.empty

        val filename = LogUtil.getLogFilename(address)

        val handler = if (useLastPage) {
            // this is intentional, aligns last page within file
            val lastPageStart = log.getHighPageAddress(expectedLength)
            FileAsyncHandler(dir.resolve(filename), lastPageStart, log.ensureWriter().allocLastPage(address + lastPageStart))
        } else {
            FileAsyncHandler(dir.resolve(filename), 0, null)
        }

        return getRemoteFile(expectedLength, filename, handler)
    }
}
