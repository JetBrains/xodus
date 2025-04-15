/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.entitystore.youtrackdb

import YTDBDatabaseProviderFactory
import com.jetbrains.youtrack.db.api.DatabaseType
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager
import com.jetbrains.youtrack.db.internal.core.exception.StorageException
import jetbrains.exodus.crypto.toByteArray
import mu.KLogging
import org.junit.After
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.nio.file.Files
import java.util.*
import kotlin.io.path.absolutePathString

@RunWith(Parameterized::class)
class EncryptedDBTest(val number: Int) {

    companion object : KLogging() {
        @JvmStatic
        @Parameterized.Parameters
        fun data(): Collection<Array<Any>> {
            return listOf(*Array(5) { arrayOf(0) }) // Repeat 20 times
        }
    }

    lateinit var provider: YTDBDatabaseProvider

    private fun createParams(encryptionKey: ByteArray?, databasePath: String): YTDBDatabaseParams {
        val password = "admin"
        val username = "admin"
        val dbName = "test"

        return YTDBDatabaseParams.builder()
            .withDatabasePath(databasePath)
            .withPassword(password)
            .withUserName(username)
            .withDatabaseType(DatabaseType.DISK)
            .withDatabaseName(dbName)
            .withCloseDatabaseInDbProvider(true)
            .apply { encryptionKey?.let { withEncryptionKey(it) } }
            .build()
    }

    @Test
    fun testEncryptedDB() {
        // Given
        val databasePath = Files.createTempDirectory("youtrackdb_test$number").absolutePathString()
        val cipherKey = UUID.randomUUID().let {
            it.mostSignificantBits.toByteArray() + it.leastSignificantBits.toByteArray()
        }
        val params = createParams(cipherKey, databasePath)
        val noEncryptionParams = createParams(null, databasePath)

        // Open encrypted
        logger.info("Connect to db and create test vertex class")

        provider = YTDBDatabaseProviderFactory.createProvider(params)
        provider.withSession { session ->
            session.schema.createVertexClass("TEST")
        }
        provider.withSession { session ->
            session.executeInTx { tx ->
                val vertex = tx.newVertex("TEST")
                vertex.setProperty("hello", "world")
            }
        }
        provider.close()
        Thread.sleep(1000)

        // Reopen the DB
        logger.info("Connect to db one more time and read")
        provider = YTDBDatabaseProviderFactory.createProvider(params)
        provider.withSession { session ->
            session.executeInTx { tx ->
                val vertex = tx.query("SELECT FROM TEST").vertexStream().toList()
                Assert.assertEquals(1, vertex.size)
            }
        }
        provider.close()
        Thread.sleep(1000)

        // Reopen the DB without the encryption key
        logger.info("Connect to db one more time without encryption")
        provider = YTDBDatabaseProviderFactory.createProvider(noEncryptionParams)
        try {
            provider.withSession { session ->
                session.executeInTx { tx ->
                    val vertex = tx.query("SELECT FROM TEST").vertexStream().toList()
                    print(vertex.size)
                }
            }
            Assert.fail("Should not open")
        } catch (_: StorageException) {
            logger.info("As expected DB failed to initialize without key")
        } catch (_: RecordNotFoundException) {
            logger.info("As expected DB failed to initialize without key")
        } catch (_: AssertionError) {
            logger.info("As expected DB failed to initialize without key")
        } catch (e: Throwable) {
            logger.error("DB failed with unexpected error", e)
            Assert.fail("Wrong error")
        }
    }

    @After
    fun close() {
        provider.close()
        try {
            if (!YouTrackDBEnginesManager.instance().isActive) {
                YouTrackDBEnginesManager.instance().startup()
            }
        } catch (_: Throwable) {
            logger.error("CANNOT REINIT YouTrackDB")
        }
    }
}
