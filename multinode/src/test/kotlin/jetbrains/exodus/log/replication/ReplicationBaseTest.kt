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
import jetbrains.exodus.env.replication.ReplicationDelta
import jetbrains.exodus.io.FileDataReader
import jetbrains.exodus.io.FileDataWriter
import jetbrains.exodus.log.*
import jetbrains.exodus.util.IOUtil
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.runBlocking
import mu.KLogging
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.auth.AnonymousCredentialsProvider
import software.amazon.awssdk.core.client.builder.ClientAsyncHttpConfiguration
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettySdkHttpClientFactory
import software.amazon.awssdk.services.s3.S3AdvancedConfiguration
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.ListBucketsRequest
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import java.io.File
import java.net.URI
import java.nio.file.Paths

abstract class ReplicationBaseTest {

    companion object : KLogging() {
        const val port = 8001
        const val openFiles = 16
        const val host = "127.0.0.1"
        const val bucket = "logfiles"
    }

    val sourceLogDir by lazy { newTmpFile() }

    val targetLogDir by lazy { newTmpFile() }

    private lateinit var api: S3Mock
    protected lateinit var httpClient: SdkAsyncHttpClient
    protected lateinit var s3: S3AsyncClient
    protected lateinit var extraHost: AwsRequestOverrideConfig

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

        httpClient = NettySdkHttpClientFactory.builder().build().createHttpClient()

        extraHost = AwsRequestOverrideConfig.builder().header("Host", "$host:$port").build()

        s3 = S3AsyncClient.builder()
                .asyncHttpConfiguration(
                        ClientAsyncHttpConfiguration.builder().httpClient(httpClient).build()
                )
                .region(Region.US_WEST_2)
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
        sourceLogDir.delete()
        targetLogDir.delete()
    }

    protected fun checkLog(log: Log, highAddress: Long, count: Long, startAddress: Long = 0, startIndex: Long = 0) {
        log.use {
            assertEquals(highAddress, log.highAddress)
            val loggables = log.getLoggableIterator(startAddress)
            var i = startIndex
            while (loggables.hasNext()) {
                val loggable = loggables.next()
                if (!NullLoggable.isNullLoggable(loggable)) { // padding possible
                    assertEquals(127, loggable.type.toInt())
                    val value = CompressedUnsignedLongByteIterable.getLong(loggable.data)
                    val expectedLength = CompressedUnsignedLongByteIterable.getIterable(i).length
                    assertEquals(i, value)
                    assertEquals(expectedLength, loggable.dataLength) // length increases since `i` is part of payload
                    i++
                }
            }

            assertEquals(count, i - startIndex)
        }
    }

    protected fun Log.writeData(iterable: ByteIterable): Long {
        return write(127.toByte(), Loggable.NO_STRUCTURE_ID, iterable)
    }

    protected fun File.createLog(fileSize: Long, releaseLock: Boolean = false, modifyConfig: LogConfig.() -> Unit = {}): Log {
        return with(LogConfig().setFileSize(fileSize)) {
            val (reader, writer) = this@createLog.createLogRW()
            this.reader = reader
            this.writer = writer
            modifyConfig()
            Log(this).also {
                if (releaseLock) { // s3mock can't open xd.lck on Windows otherwise
                    writer.release()
                }
            }
        }
    }

    protected fun File.createLogRW() = Pair(FileDataReader(this, openFiles), FileDataWriter(this))

    protected fun newTmpFile(): File {
        return File(TestUtil.createTempDir(), bucket).also {
            it.mkdirs()
        }
    }

    protected suspend fun doDebug(s3: S3AsyncClient) {
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

    fun Log.appendLog(delta: ReplicationDelta) {
        LogAppender.appendLog(
                this,
                delta,
                makeFileFactory()
        )()
    }

    protected open fun Log.makeFileFactory(): FileFactory {
        return S3FileFactory(s3, Paths.get(location), bucket, extraHost)
    }

    fun writeToLog(sourceLog: Log, count: Long, startIndex: Long = 0) {
        sourceLog.beginWrite()
        for (i in startIndex until count + startIndex) {
            sourceLog.writeData(CompressedUnsignedLongByteIterable.getIterable(i.toLong()))
        }
        sourceLog.flush()
        sourceLog.endWrite()

        runBlocking {
            doDebug(s3)
        }

    }

    fun Log.filesDelta(startAddress: Long): LongArray {
        return tip.getFilesFrom(startAddress).asSequence().toList().toLongArray()
    }


    fun newLogs(fileSize: Long = 4L): Pair<Log, Log> {
        val sourceLog = sourceLogDir.createLog(fileSize, releaseLock = true) {
            cachePageSize = 1024
        }
        val targetLog = targetLogDir.createLog(fileSize) {
            cachePageSize = 1024
        }

        runBlocking {
            doDebug(s3)
        }

        return sourceLog to targetLog
    }
}
