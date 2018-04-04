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
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.nio.file.Path

class S3FileFactory(
        private val s3: S3AsyncClient,
        private val dir: Path,
        private val bucket: String,
        private val requestOverrideConfig: AwsRequestOverrideConfig? = null
) : FileFactory {

    override fun fetchFile(log: Log, address: Long, expectedLength: Long, lastPage: ByteArray?): WriteResult {
        if (expectedLength < 0L || expectedLength > log.fileLengthBound) {
            throw IllegalArgumentException("Incorrect expected length specified")
        }
        if (expectedLength == 0L) {
            return WriteResult(0, 0)
        }
        val filename = LogUtil.getLogFilename(address)
        // if target log is appended in the meantime, ignore appended bytes thanks to S3 API Range header support
        // https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        return s3.getObject(
                GetObjectRequest.builder().range("bytes=0-${expectedLength - 1}")
                        .requestOverrideConfig(requestOverrideConfig).bucket(bucket).key(filename).build(),
                FileAsyncHandler(dir.resolve(filename), lastPage?.let {
                    log.getHighPageAddress(expectedLength) // this is intentional, aligns last page within file
                } ?: 0, lastPage)
        ).get()
    }
}
