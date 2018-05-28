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

import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataReaderWriterProvider
import jetbrains.exodus.io.DataWriter
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client

class S3DataReaderWriterProvider @JvmOverloads constructor(
        private val s3: S3AsyncClient,
        private val s3Sync: S3Client,
        private val requestOverrideConfig: AwsRequestOverrideConfig? = null) : DataReaderWriterProvider() {
    constructor() : this(S3AsyncClient.builder().region(Region.EU_WEST_1).build(), S3Client.builder().region(Region.EU_WEST_1).build()) // System.getProperty("exodus.s3.bucket.name")

    override fun newReaderWriter(location: String): Pair<DataReader, DataWriter> {
        val writer = S3DataWriter(s3, s3Sync, location, requestOverrideConfig)
        return Pair(S3DataReader(s3, location, requestOverrideConfig, writer), writer)
    }
}
