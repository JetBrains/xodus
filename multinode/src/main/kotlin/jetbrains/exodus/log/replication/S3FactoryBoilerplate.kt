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
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.async.AsyncResponseHandler
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import java.util.concurrent.CompletableFuture

interface S3FactoryBoilerplate {
    val s3: S3AsyncClient
    val bucket: String
    val requestOverrideConfig: AwsRequestOverrideConfig?

    fun <T> getRemoteFile(length: Long, startingLength: Long, name: String, handler: AsyncResponseHandler<GetObjectResponse, T>): CompletableFuture<T> {
        // if target log is appended in the meantime, ignore appended bytes thanks to S3 API Range header support
        // https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        return s3.getObject(
                GetObjectRequest.builder().range("bytes=$startingLength-${length - 1}")
                        .requestOverrideConfig(requestOverrideConfig).bucket(bucket).key(name).build(), handler
        )
    }

    fun checkPreconditions(log: Log, expectedLength: Long, startingLength: Long): Boolean {
        if (expectedLength < startingLength || expectedLength > log.fileLengthBound) {
            throw IllegalArgumentException("Incorrect expected length specified")
        }
        if (expectedLength == startingLength) {
            return true
        }
        return false
    }
}
