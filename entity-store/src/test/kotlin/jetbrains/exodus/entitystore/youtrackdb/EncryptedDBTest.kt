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
import YouTrackDBConfigFactory
import YouTrackDBFactory
import com.jetbrains.youtrack.db.api.DatabaseType
import com.jetbrains.youtrack.db.api.YouTrackDB
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
            return Arrays.asList<Array<Any>>(*Array(5) { arrayOf(0) }) // Repeat 20 times
        }
    }

    lateinit var provider: YTDBDatabaseProvider
    lateinit var db: YouTrackDB

    private fun createConfig(key: ByteArray?): YTDBDatabaseConfig {
        val password = "admin"
        val username = "admin"
        val dbName = "test"

        val connConfig = YTDBDatabaseConnectionConfig.builder()
            .withPassword(password)
            .withUserName(username)
            .withDatabaseType(DatabaseType.PLOCAL)
            .withDatabaseRoot(Files.createTempDirectory("oxigenDB_test$number").absolutePathString())
            .build()

        return YTDBDatabaseConfig.builder()
            .withConnectionConfig(connConfig)
            .withDatabaseType(DatabaseType.PLOCAL)
            .withDatabaseName(dbName)
            .withCipherKey(key)
            .build()
    }

    @Test
    fun testEncryptedDB() {

        val cipherKey = UUID.randomUUID().let {
            it.mostSignificantBits.toByteArray() + it.leastSignificantBits.toByteArray()
        }
        val config = createConfig(cipherKey)
        val noEncryptionConfig = createConfig(null)
        logger.info("Connect to db and create test vertex class")
        val dbConfig = YouTrackDBConfigFactory.createDefaultDBConfig(config)
        db = YouTrackDBFactory.initYouTrackDb(config, dbConfig)
        provider = YTDBDatabaseProviderFactory.createProvider(config, db, dbConfig)
        provider.withSession { session ->
            session.createVertexClass("TEST")
        }
        logger.info("Set vertex property")
        provider.withSession { session ->
            session.executeInTx {
                val vertex = session.newVertex("TEST")
                vertex.setProperty("hello", "world")
                vertex.save()
            }
        }
        db.close()
        logger.info("Close the DB")
        Thread.sleep(1000)
        logger.info("Connect to db one more time and read")
        db = YouTrackDBFactory.initYouTrackDb(config, dbConfig)
        provider = YTDBDatabaseProviderFactory.createProvider(config, db, dbConfig)
        provider.withSession { session ->
            session.executeInTx {
                val vertex = session.query("SELECT FROM TEST").vertexStream().toList()
                Assert.assertEquals(1, vertex.size)
            }
        }
        logger.info("Close the DB")
        db.close()
        Thread.sleep(1000)
        logger.info("Connect to db one more time without encryption")
        db = YouTrackDBFactory.initYouTrackDb(config, dbConfig)
        try {
            YTDBDatabaseProviderFactory.createProvider(noEncryptionConfig, db, dbConfig).apply {
                withSession { session ->
                    session.executeInTx {
                        val vertex = session.query("SELECT FROM TEST").vertexStream().toList()
                        print(vertex.size)
                    }
                }
                Assert.fail("Should not open")
            }
        } catch (_: StorageException) {
            logger.info("As expected DB failed to initialize without key")
        }catch (_: RecordNotFoundException) {
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
        db.close()
        try {
            if (!YouTrackDBEnginesManager.instance().isActive) {
                YouTrackDBEnginesManager.instance().startup()
            }
        } catch (_: Throwable) {
            logger.error("CANNOT REINIT YouTrackDB")
        }
    }
}
