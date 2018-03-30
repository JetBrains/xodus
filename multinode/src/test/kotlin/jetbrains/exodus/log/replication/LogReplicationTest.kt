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

import io.findify.s3mock.S3Mock
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.TestUtil
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataWriter
import jetbrains.exodus.io.FileDataReader
import jetbrains.exodus.io.FileDataWriter
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.util.IOUtil
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import org.junit.*
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.auth.AnonymousCredentialsProvider
import software.amazon.awssdk.core.client.builder.ClientAsyncHttpConfiguration
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.http.nio.netty.NettySdkHttpClientFactory
import software.amazon.awssdk.services.s3.S3AdvancedConfiguration
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.ListBucketsRequest
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import java.io.File
import java.net.URI

class LogReplicationTest {
    companion object: KLogging() {
        const val port = 8001
        const val openFiles = 16
        const val host = "127.0.0.1"
        const val bucket = "logfiles"
    }

    private val sourceLogDir by lazy {
        newTmpFile()
    }

    private val targetLogDir by lazy {
        newTmpFile()
    }

    private lateinit var api: S3Mock
    private lateinit var s3: S3AsyncClient
    private lateinit var extraHost: AwsRequestOverrideConfig

    @Before
    fun setUp() {
        if (sourceLogDir.exists()) {
            IOUtil.deleteRecursively(sourceLogDir)
        }
        if (targetLogDir.exists()) {
            IOUtil.deleteRecursively(targetLogDir)
        }

        api = S3Mock.Builder().withPort(port).withFileBackend(sourceLogDir.parentFile.absolutePath).build().apply {
            start()
        }

        val region = Region.US_WEST_2
        val httpClient = NettySdkHttpClientFactory.builder().build().createHttpClient()

        extraHost = AwsRequestOverrideConfig.builder().header("Host", "$host:$port").build()

        s3 = S3AsyncClient.builder()
                .asyncHttpConfiguration(
                        ClientAsyncHttpConfiguration.builder().httpClient(httpClient).build()
                )
                .region(region)
                .endpointOverride(URI("http://$host:$port"))
                .advancedConfiguration(S3AdvancedConfiguration.builder().pathStyleAccessEnabled(true).build()) // for tests only
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build()
    }

    @After
    fun close() {
        if (::api.isInitialized) {
            api.shutdown()
        }
        if (::s3.isInitialized) {
            s3.close()
        }
    }

    @Ignore
    @Test
    fun testSimple() {
        val sourceLog = sourceLogDir.createLog(fileSize = 4L, releaseLock = true) {
            cachePageSize = 1024
        }
        val targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        runBlocking {
            doDebug(s3)
        }

        val count = 10
        sourceLog.beginWrite()
        for (i in 0 until count) {
            sourceLog.writeData(CompressedUnsignedLongByteIterable.getIterable(i.toLong()))
        }
        sourceLog.flush()
        sourceLog.endWrite()

        runBlocking {
            doDebug(s3)
        }

        val it = targetLog.getLoggableIterator(0)
        var i = 0
        while (it.hasNext()) {
            val l = it.next()
            Assert.assertEquals((4 * i++).toLong(), l.address)
            Assert.assertEquals(127, l.type.toLong())
            Assert.assertEquals(1, l.dataLength.toLong())
        }

        try {
            Assert.assertEquals(count.toLong(), i.toLong())
        } finally {
            sourceLog.close()
            targetLog.close()
        }
    }

    private fun Log.writeData(iterable: ByteIterable): Long {
        return write(127.toByte(), Loggable.NO_STRUCTURE_ID, iterable)
    }

    private fun File.createLog(fileSize: Long, releaseLock: Boolean = false, modifyConfig: LogConfig.() -> Unit = {}): Log {
        return with(LogConfig().setFileSize(fileSize)) {
            val (reader, writer) = this@createLog.createLogRW()
            setReader(reader)
            setWriter(writer)
            modifyConfig()
            Log(this).also {
                if (releaseLock) { // s3mock can't open xd.lck on Windows otherwise
                    writer.release()
                }
            }
        }
    }

    private fun File.createLogRW(): Pair<DataReader, DataWriter> {
        return Pair(FileDataReader(this, openFiles), FileDataWriter(this))
    }

    private fun newTmpFile(): File {
        return File(TestUtil.createTempDir(), bucket).also {
            it.mkdirs()
        }
    }

    private suspend fun doDebug(s3: S3AsyncClient) {
        logger.info { "Listing buckets\n" }
        val bucketsResponse = s3.listBuckets(ListBucketsRequest.builder().requestOverrideConfig(extraHost).build()).await()

        logger.info {
            buildString {
                append("Buckets list:\n")
                for (bucket in bucketsResponse.buckets()) {
                    append("    ")
                    append(bucket.name())
                    append('\n')
                }
            }
        }

        logger.info { "Listing objects\n" }

        val objectListing = s3.listObjects(ListObjectsRequest.builder()
                .requestOverrideConfig(extraHost)
                .bucket(bucket)
                .delimiter("/")
                //.prefix("My")
                .build()).await()
        logger.info {
            buildString {
                append("Objects list:\n")
                for (objectSummary in objectListing.contents()) {
                    append("    ")
                    append(objectSummary.key())
                    append(" ")
                    append("(size = ")
                    append(objectSummary.size())
                    append(")\n")
                }
            }
        }
    }
}
