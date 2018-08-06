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
package jetbrains.exodus.env

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.AnonymousAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.findify.s3mock.S3Mock
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataWriter
import jetbrains.exodus.log.ReplicatedLogTestMixin
import jetbrains.exodus.log.replication.S3DataReaderWriterProvider
import mu.KLogging
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.auth.AnonymousCredentialsProvider
import software.amazon.awssdk.core.client.builder.ClientAsyncHttpConfiguration
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettySdkHttpClientFactory
import software.amazon.awssdk.services.s3.S3AdvancedConfiguration
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import java.io.File
import java.net.URI
import java.time.Duration


class EnvironmentS3Test : EnvironmentTest(), ReplicatedLogTestMixin {
    companion object : KLogging() {
        const val host = "127.0.0.1"
    }

    val logDir by lazy { newTmpFile() }

    private lateinit var api: S3Mock
    protected lateinit var httpClient: SdkAsyncHttpClient
    protected lateinit var s3: S3AsyncClient
    protected lateinit var s3Sync: S3Client
    protected lateinit var extraHost: AwsRequestOverrideConfig

    override fun setUp() {
        initMocks()
        super.setUp()
    }

    override fun createRW(): Pair<DataReader, DataWriter> {
        return S3DataReaderWriterProvider(s3, s3Sync, extraHost).newReaderWriter("logfiles")
    }

    override fun tearDown() {
        super.tearDown()
        deinitMocks()
        logDir.delete()
    }

    override fun archiveDB(target: String) {
        EnvironmentTestsBase.archiveDB(File(logDir, "logfiles").path, target)
    }

    private fun deinitMocks() {
        if (::api.isInitialized) {
            val api = api
            api.shutdown()
        }
        if (::s3.isInitialized) {
            s3.close()
        }
    }

    private fun initMocks() {
        api = S3Mock.Builder().withPort(0).withInMemoryBackend().build()
        val port = api.start().localAddress().port

        httpClient = NettySdkHttpClientFactory.builder().readTimeout(Duration.ofSeconds(4)).build().createHttpClient()

        extraHost = AwsRequestOverrideConfig.builder().header("Host", "$host:$port").build()

        s3Sync = S3Client.builder().region(Region.US_WEST_2)
                .endpointOverride(URI("http://$host:$port"))
                .advancedConfiguration(S3AdvancedConfiguration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build()

        s3 = S3AsyncClient.builder()
                .asyncHttpConfiguration(
                        ClientAsyncHttpConfiguration.builder().httpClient(httpClient).build()
                )
                //.overrideConfiguration(ClientOverrideConfiguration.builder().advancedOption(AdvancedClientOption.SIGNER_PROVIDER, NoOpSignerProvider()).build())
                .region(Region.US_WEST_2)
                .endpointOverride(URI("http://$host:$port"))
                .advancedConfiguration(S3AdvancedConfiguration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build()

        // can't create bucket with async client
        val client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://$host:$port", "us-west-2"))
                .withCredentials(AWSStaticCredentialsProvider(AnonymousAWSCredentials()))
                .build()
        client.also {
            client.createBucket("logfiles")
            client.shutdown()
        }
    }
}
