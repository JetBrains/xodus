/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.log.replication

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*
import java.nio.ByteBuffer

internal const val deletePackSize = 500

fun ByteBuffer.copyBytes(skip: Int, output: ByteArray, offset: Int, length: Int) {
    if (hasArray()) {
        System.arraycopy(
                array(),
                arrayOffset() + position() + skip,
                output,
                offset,
                length
        )
    }

    asReadOnlyBuffer().let { ro ->
        ro.position(ro.position() + skip)
        ro.get(output, offset, length)
    }
}

internal fun listObjectsBuilder(bucketName: String, requestOverrideConfig: AwsRequestOverrideConfiguration? = null): ListObjectsRequest.Builder {
    return ListObjectsRequest.builder()
            .overrideConfiguration(requestOverrideConfig)
            .bucket(bucketName)
}

internal fun listObjects(s3: S3AsyncClient, builder: ListObjectsRequest.Builder): Sequence<S3Object> {
    return sequence {
        while (true) {
            val response = s3.listObjects(builder.build()).get()
            val contents = response.contents() ?: break
            if (contents.isEmpty()) {
                break
            }
            yieldAll(contents)
            val last = contents.last()
            if (!response.isTruncated && response.nextMarker().isNullOrEmpty()) {
                break
            }
            builder.marker(last.key())
        }
    }
}

internal fun List<String>.deleteS3Objects(s3: S3AsyncClient,
                                          bucketName: String,
                                          requestOverrideConfig: AwsRequestOverrideConfiguration? = null) {
    S3DataReader.logger.info { "deleting files ${joinToString()}" }

    val deleteObjectsResponse = s3.deleteObjects(DeleteObjectsRequest.builder()
            .overrideConfiguration(requestOverrideConfig)
            .delete(
                    Delete.builder().objects(
                            map { ObjectIdentifier.builder().key(it).build() })
                            .build())
            .bucket(bucketName)
            .build()).get()

    if (deleteObjectsResponse.errors()?.isNotEmpty() == true) {
        throw IllegalStateException("Can't delete files")
    }
}