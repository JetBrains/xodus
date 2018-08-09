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
import jetbrains.exodus.core.dataStructures.persistent.read
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.LogUtil.LOG_BLOCK_ALIGNMENT
import mu.KLogging
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Object
import java.net.URI

class S3DataReaderTest {
    companion object : KLogging() {
        const val host = "127.0.0.1"
        const val bucket = "logfiles"
    }

    private lateinit var api: S3Mock
    private lateinit var httpClient: SdkAsyncHttpClient
    private lateinit var s3: S3AsyncClient
    private lateinit var s3Sync: S3Client
    private lateinit var extraHost: AwsRequestOverrideConfiguration


    @Before
    fun setup() {
        api = S3Mock.Builder().withPort(0).withInMemoryBackend().build()
        val port = api.start().localAddress().port

        httpClient = NettyNioAsyncHttpClient.builder().build()

        extraHost = AwsRequestOverrideConfiguration.builder().putHeader("Host", "$host:$port").build()

        s3Sync = S3Client.builder().region(Region.US_WEST_2)
                .endpointOverride(URI("http://$host:$port"))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()) // for minio
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build()

        s3 = S3AsyncClient.builder()
                .httpClient(httpClient)
                .region(Region.US_WEST_2)
                .endpointOverride(URI("http://$host:$port"))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()) // for minio
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build()

        s3Sync.createBucket(CreateBucketRequest.builder().bucket("logfiles").build())
    }

    @After
    fun cleanup() {
        if (::api.isInitialized) {
            api.shutdown()
        }
        if (::s3.isInitialized) {
            s3.close()
        }
        if (::httpClient.isInitialized) {
            httpClient.close()
        }
    }

    @Test
    fun `should read simple xd-files`() {
        newXdObject(0)
        newXdObject(1)
        newXdObject(2, 100) // total length 356
        with(newReader()) {
            val blocks = blocks.toList()
            assertEquals(3, blocks.size)
            assertEquals(0, blocks[0].address.toInt())
            assertEquals(LOG_BLOCK_ALIGNMENT, blocks[1].address.toInt())
            assertEquals(2 * LOG_BLOCK_ALIGNMENT, blocks[2].address.toInt())
        }
    }

    @Test
    fun `should read xd-files with partiall folders`() {
        newXdObject(0)
        newXdObject(1)
        newFolderObject(2, 100) // total length 356
        with(newReader()) {
            val blocks = blocks.toList()
            assertEquals(3, blocks.size)
            assertEquals(0, blocks[0].address.toInt())
            assertEquals(LOG_BLOCK_ALIGNMENT, blocks[1].address.toInt())
            assertEquals(2 * LOG_BLOCK_ALIGNMENT, blocks[2].address.toInt())
        }
    }

    @Test
    fun `xd-files should have higher priority then partially folders`() {
        newXdObject(0)
        newXdObject(1)
        newXdObject(2)
        newFolderObject(1)
        with(newReader()) {
            val blocks = blocks.toList()
            assertEquals(3, blocks.size)
            assertEquals(0, blocks[0].address.toInt())
            assertEquals(LOG_BLOCK_ALIGNMENT, blocks[1].address.toInt())
            assertEquals(2 * LOG_BLOCK_ALIGNMENT, blocks[2].address.toInt())
            assertTrue(blocks.all { it is S3Block })
        }
    }

    @Test
    fun `should handle partially files`() {
        newXdObject(0)
        newXdObject(1)
        newFolderObject(2, 100) // total length 356
        with(newReader()) {
            val blocks = blocks.toList()
            assertEquals(3, blocks.size)
            assertEquals(0, blocks[0].address.toInt())
            assertEquals(LOG_BLOCK_ALIGNMENT, blocks[1].address.toInt())
            assertEquals(2 * LOG_BLOCK_ALIGNMENT, blocks[2].address.toInt())
        }
    }

    @Test
    fun `should delete files on clear`() {
        newXdObject(0)
        newFolderObject(1, 100)
        with(newReader()) {
            writer.clear()
            assertTrue(s3Objects?.none { it.key().endsWith(".xd") } ?: true)
        }
    }

    @Test
    @Ignore
    fun `should delete files and folder for deleting blocks`() {
        newXdObject(0)
        newXdObject(1)
        newFolderObject(1)
        with(newReader()) {
            writer.removeBlock(LOG_BLOCK_ALIGNMENT.toLong(), RemoveBlockType.Delete)
            assertEquals(1, s3Objects?.size)
        }
    }

    @Test
    @Ignore
    fun `should rename files and folder for renaming blocks`() {
        newXdObject(0)
        newXdObject(1)
        newFolderObject(1)
        with(newReader()) {
            writer.removeBlock(LOG_BLOCK_ALIGNMENT.toLong(), RemoveBlockType.Rename)
            assertEquals(1, s3Objects?.filter { it.key().endsWith(".xd") }?.size)
            assertEquals(2, s3Objects?.filter { it.key().endsWith(".del") }?.size)
        }
    }

    @Test
    @Ignore
    fun `should truncate files and folder`() {
        val file0 = newXdObject(0)
        val file1 = newXdObject(1)
        var file2: String? = null
        newFolderObject(1).also {
            file2 = it.newPartialXd(1)
            it.newPartialXd(2)
            it.newPartialXd(3, 100)
        }
        with(newReader()) {
            writer.truncateBlock(LOG_BLOCK_ALIGNMENT.toLong(), 100)

            s3Objects.let {
                assertNotNull(it)
                assertEquals(3, it!!.size)
                with(it.first { it.key() == file0 }) {
                    assertEquals(LOG_BLOCK_ALIGNMENT, size().toInt())
                }

                with(it.first { it.key() == file1 }) {
                    assertEquals(100, size().toInt())
                }

                with(it.first { it.key() == file2!! }) {
                    assertEquals(100, size().toInt())
                }
            }
        }
    }

    @Test
    fun `should read from xd-files`() {
        newXdObject(0)
        newXdObject(1, 100)
        with(newReader()) {
            with(ByteArray(LOG_BLOCK_ALIGNMENT) { 0 }) {
                getBlock(0).read(this, 0, 0, LOG_BLOCK_ALIGNMENT)
                assertReadAt(0..(LOG_BLOCK_ALIGNMENT - 1))
            }

            with(ByteArray(200) { 0 }) {
                getBlock(LOG_BLOCK_ALIGNMENT.toLong()).read(this, 0, 10, 100)
                assertReadAt(10..109)
            }
        }
    }

    @Test
    fun `should ignore blobs and textindex folders`() {
        newXdObject(0)
        newXdObject(LogUtil.getLogFilename(0).replace(".xd", ".del"))
        "blobs".newXdObject(1)
        "textindex".newXdObject(2)
        with(newReader()) {
            with(blocks.toList()) {
                assertEquals(1, size)
                assertTrue(get(0) is S3Block)
            }
        }
    }

    @Test
    fun `should ignore unexpected files in partially folders`() {
        newFolderObject(0).also {
            it.newXdObject("_xd.xd") // some trash
            it.newXdObject(LogUtil.getLogFilename(0).replace(".xd", ".del"))  // submitted for deletion file
        }
        with(newReader()) {
            with(blocks.toList()) {
                assertEquals(1, size)
                val block = get(0) as S3FolderBlock
                assertEquals(1, block.blocks.read { size })
                assertEquals(LOG_BLOCK_ALIGNMENT.toLong(), block.length())
            }
        }
    }

    @Test
    fun `should read from partially folders`() {
        newFolderObject(0).also {
            it.newPartialXd(0)
            it.newPartialXd(1)
            it.newPartialXd(2)
        }
        newFolderObject(3).also {
            it.newPartialXd(3)
            it.newPartialXd(4)
            it.newPartialXd(5, 100)
        }
        with(newReader()) {
            // should read from first file in folder
            with(ByteArray(300) { 0 }) {
                getBlock(0).read(this, 0, 100, 200)
                assertReadAt(100..299)
            }

            // should read from two files in folder
            with(ByteArray(200 + LOG_BLOCK_ALIGNMENT) { 0 }) {
                getBlock(0).read(this, 0, 100, LOG_BLOCK_ALIGNMENT)
                assertReadAt(100..(LOG_BLOCK_ALIGNMENT + 99))
            }

            // should read from few files
            with(ByteArray(4 * LOG_BLOCK_ALIGNMENT) { 0 }) {
                getBlock(LOG_BLOCK_ALIGNMENT.toLong() * 3).read(this, 0, 100, 2 * LOG_BLOCK_ALIGNMENT)
                assertReadAt(100..(2 * LOG_BLOCK_ALIGNMENT + 99))
            }
        }
    }

    private fun ByteArray.assertReadAt(range: IntRange) {
        forEachIndexed { index, byte ->
            if (index in range) {
                assertEquals("expected 1 at $index position", 1.toByte(), byte)
            } else {
                assertEquals("expected 0 at $index position", 0.toByte(), byte)
            }
        }
    }

    private fun newXdObject(number: Long, size: Int = LOG_BLOCK_ALIGNMENT): String {
        val key = LogUtil.getLogFilename(LOG_BLOCK_ALIGNMENT * number)
        newObject(key, size)
        return key
    }

    private fun String.newXdObject(number: Long, size: Int = LOG_BLOCK_ALIGNMENT): String {
        return newXdObject(LogUtil.getLogFilename(LOG_BLOCK_ALIGNMENT * number), size)
    }

    private fun String.newXdObject(fileName: String, size: Int = LOG_BLOCK_ALIGNMENT): String {
        val key = this + "/" + fileName
        newObject(key, size)
        return key
    }

    private fun newXdObject(fileName: String, size: Int = LOG_BLOCK_ALIGNMENT): String {
        newObject(fileName, size)
        return fileName
    }

    private fun String.newPartialXd(number: Long, size: Int = LOG_BLOCK_ALIGNMENT): String {
        val key = "_" + this + "/" + getPartialFileName(LOG_BLOCK_ALIGNMENT * number)
        newObject(key, size)
        return key
    }

    private fun newFolderObject(number: Long, size: Int = LOG_BLOCK_ALIGNMENT): String {
        return LogUtil.getLogFilename(LOG_BLOCK_ALIGNMENT * number).replace(".xd", "").also {
            it.newPartialXd(LOG_BLOCK_ALIGNMENT * number, size)
        }
    }

    private fun newObject(key: String, size: Int) {
        s3Sync.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentLength(size.toLong())
                        .build(),
                RequestBody.fromBytes(ByteArray(size) { 1 })
        )
    }

    private fun newReader() = S3DataReader(s3, bucket, extraHost, S3DataWriter(s3Sync, s3, bucket, extraHost))

    private val s3Objects: List<S3Object>?
        get() {
            val builder = ListObjectsRequest.builder()
                    .overrideConfiguration(extraHost)
                    .bucket(bucket)
            return s3.listObjects(builder.build()).get()?.contents()
        }

    private fun getPartialFileName(address: Long): String {
        return String.format("%016x${LogUtil.LOG_FILE_EXTENSION}", address)
    }
}
