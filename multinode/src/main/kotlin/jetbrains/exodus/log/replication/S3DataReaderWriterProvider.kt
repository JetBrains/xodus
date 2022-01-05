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

import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataReaderWriterProvider
import jetbrains.exodus.io.DataWriter
import jetbrains.exodus.kotlin.DefaultDelegate
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client

class S3DataReaderWriterProvider @JvmOverloads constructor(
        private val requestOverrideConfig: AwsRequestOverrideConfiguration? = null) : DataReaderWriterProvider() {

    var s3 by DefaultDelegate<S3AsyncClient> {
        S3AsyncClient.builder().region(Region.EU_WEST_1).build()
    }
    var s3Sync by DefaultDelegate<S3Client> {
        S3Client.builder().region(Region.EU_WEST_1).build()
    }

    private var env: EnvironmentImpl? = null

    override fun onEnvironmentCreated(environment: Environment) {
        super.onEnvironmentCreated(environment)
        this.env = environment as EnvironmentImpl
    }

    override fun newReaderWriter(location: String): Pair<DataReader, DataWriter> {
        val writer = S3DataWriter(s3Sync, s3, location, requestOverrideConfig, env?.log)
        return Pair(S3DataReader(s3, location, requestOverrideConfig, writer), writer)
    }

}