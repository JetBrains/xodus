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
import jetbrains.exodus.TestUtil
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.LogUtil.LOG_BLOCK_ALIGNMENT
import mu.KLogging
import org.junit.After
import org.junit.Assert.*
import org.junit.Assume.assumeFalse
import org.junit.Before
import org.junit.Test
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.auth.AnonymousCredentialsProvider
import software.amazon.awssdk.core.client.builder.ClientAsyncHttpConfiguration
import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettySdkHttpClientFactory
import software.amazon.awssdk.services.s3.S3AdvancedConfiguration
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsRequest
import software.amazon.awssdk.services.s3.model.S3Object
import java.io.File
import java.net.URI
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Paths

class S3DataReaderTest {
    companion object : KLogging() {
        const val host = "127.0.0.1"
        const val bucket = "logfiles"
    }

    private lateinit var api: S3Mock
    private lateinit var httpClient: SdkAsyncHttpClient
    private lateinit var s3: S3AsyncClient
    private lateinit var s3Sync: S3Client
    private lateinit var extraHost: AwsRequestOverrideConfig

    private val sourceDir by lazy { newTmpFile() }

    private fun newTmpFile(): File {
        return File(TestUtil.createTempDir(), bucket).also {
            it.mkdirs()
        }
    }

    @Before
    fun setup() {
        api = S3Mock.Builder().withPort(0).withFileBackend(sourceDir.parentFile.absolutePath).build()
        val port = api.start().localAddress().port

        httpClient = NettySdkHttpClientFactory.builder().build().createHttpClient()

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
                .region(Region.US_WEST_2)
                .endpointOverride(URI("http://$host:$port"))
                .advancedConfiguration(
                        S3AdvancedConfiguration.builder().pathStyleAccessEnabled(true).build()
                )
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build()
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
        sourceDir.delete()
    }

    @Test
    fun `should read simple xd-files`() {
        sourceDir.newDBFile(0)
        sourceDir.newDBFile(1)
        sourceDir.newDBFile(2, 100) // total length 356
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
        sourceDir.newDBFile(0)
        sourceDir.newDBFile(1)
        newDBFolder(2, 100) // total length 356
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
        sourceDir.newDBFile(0)
        sourceDir.newDBFile(1)
        sourceDir.newDBFile(2)
        newDBFolder(1)
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
        sourceDir.newDBFile(0)
        sourceDir.newDBFile(1)
        newDBFolder(2, 100) // total length 356
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
        assumeFalse(isWindows)
        sourceDir.newDBFile(0)
        newDBFolder(1, 100)
        with(newReader()) {
            writer.clear()
            assertTrue(s3Objects?.none { it.key().endsWith(".xd") } ?: true)
        }
    }

    @Test
    fun `should delete files and folder for deleting blocks`() {
        assumeFalse(isWindows)

        sourceDir.newDBFile(0)
        sourceDir.newDBFile(1)
        newDBFolder(1)
        with(newReader()) {
            writer.removeBlock(LOG_BLOCK_ALIGNMENT.toLong(), RemoveBlockType.Delete)
            assertEquals(1, s3Objects?.size)
        }
    }

    @Test
    fun `should rename files and folder for renaming blocks`() {
        assumeFalse(isWindows)

        sourceDir.newDBFile(0)
        sourceDir.newDBFile(1)
        newDBFolder(1)
        with(newReader()) {
            writer.removeBlock(LOG_BLOCK_ALIGNMENT.toLong(), RemoveBlockType.Rename)
            assertEquals(1, s3Objects?.filter { it.key().endsWith(".xd") }?.size)
            assertEquals(2, s3Objects?.filter { it.key().endsWith(".del") }?.size)
        }
    }

    @Test
    fun `should truncate files and folder`() {
        assumeFalse(isWindows)

        val file0 = sourceDir.newDBFile(0)
        val file1 = sourceDir.newDBFile(1)
        var file2: String? = null
        newDBFolder(1).also {
            file2 = it.newPartialFile(1)
            it.newPartialFile(2)
            it.newPartialFile(3, 100)
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
        sourceDir.newDBFile(0)
        sourceDir.newDBFile(1, 100)
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
        sourceDir.newDBFile(0)
        sourceDir.newDBFile(LogUtil.getLogFilename(0).replace(".xd", ".del"))
        File(sourceDir, "blobs").also { it.mkdirs() }.newDBFile(1)
        File(sourceDir, "textindex").also { it.mkdirs() }.newDBFile(2)
        with(newReader()) {
            with(blocks.toList()) {
                assertEquals(1, size)
                assertTrue(get(0) is S3Block)
            }
        }
    }

    @Test
    fun `should ignore unexpected files in partially folders`() {
        newDBFolder(0).also {
            it.newDBFile("_xd.xd") // some trash
            it.newDBFile(LogUtil.getLogFilename(0).replace(".xd", ".del"))  // submitted for deletion file
        }
        with(newReader()) {
            with(blocks.toList()) {
                assertEquals(1, size)
                val block = get(0) as S3FolderBlock
                assertEquals(1, block.blocks.size)
                assertEquals(LOG_BLOCK_ALIGNMENT.toLong(), block.length())
            }
        }
    }

    @Test
    fun `should read from partially folders`() {
        newDBFolder(0).also {
            it.newPartialFile(0)
            it.newPartialFile(1)
            it.newPartialFile(2)
        }
        newDBFolder(3).also {
            it.newPartialFile(3)
            it.newPartialFile(4)
            it.newPartialFile(5, 100)
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

    private fun File.newDBFile(number: Long, size: Int = LOG_BLOCK_ALIGNMENT): String {
        val prefix = if (name == bucket) "" else "$name/"
        return prefix + File(this, LogUtil.getLogFilename(LOG_BLOCK_ALIGNMENT * number)).also {
            it.createNewFile()
            val path = Paths.get(it.toURI())
            Files.write(path, ByteArray(size) { 1 })
        }.name
    }

    private fun File.newDBFile(fileName: String, size: Int = LOG_BLOCK_ALIGNMENT): String {
        val prefix = if (name == bucket) "" else "$name/"
        return prefix + File(this, fileName).also {
            it.createNewFile()
            val path = Paths.get(it.toURI())
            Files.write(path, ByteArray(size) { 1 })
        }.name
    }

    private fun File.newPartialFile(number: Long, size: Int = LOG_BLOCK_ALIGNMENT): String {
        val prefix = if (name == bucket) "" else "$name/"
        return prefix + File(this, getPartialFileName(LOG_BLOCK_ALIGNMENT * number)).also {
            it.createNewFile()
            val path = Paths.get(it.toURI())
            Files.write(path, ByteArray(size) { 1 })
        }.name
    }

    private fun newDBFolder(number: Long, size: Int = LOG_BLOCK_ALIGNMENT): File {
        return sourceDir.let {
            val name = LogUtil.getLogFilename(LOG_BLOCK_ALIGNMENT * number).replace(".xd", "")
            File(it.absolutePath + File.separator + "_" + name).also {
                it.mkdir()
                it.newPartialFile(LOG_BLOCK_ALIGNMENT * number, size)
            }
        }
    }

    private fun newReader() = S3DataReader(s3, bucket, extraHost, S3DataWriter(s3Sync, s3, bucket, extraHost))

    private val s3Objects: List<S3Object>?
        get() {
            val builder = ListObjectsRequest.builder()
                    .requestOverrideConfig(extraHost)
                    .bucket(bucket)
            return s3.listObjects(builder.build()).get()?.contents()
        }
    private val isWindows: Boolean
        get() {
            return try {
                FileChannel.open(sourceDir.toPath())
                false
            } catch (e: Exception) {
                true
            }
        }

    private fun getPartialFileName(address: Long): String {
        return String.format("%016x${LogUtil.LOG_FILE_EXTENSION}", address)
    }
}
