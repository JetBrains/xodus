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
package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.Orient
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.exception.OStorageException
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.crypto.toByteArray
import mu.KLogging
import org.junit.After
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.lang.AssertionError
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

    lateinit var provider: ODatabaseProviderImpl
    lateinit var db: OrientDB

    private fun createConfig(key: ByteArray?): ODatabaseConfig {
        val password = "admin"
        val username = "admin"
        val dbName = "test"

        val connConfig = ODatabaseConnectionConfig.builder()
            .withPassword(password)
            .withUserName(username)
            .withDatabaseType(ODatabaseType.PLOCAL)
            .withDatabaseRoot(Files.createTempDirectory("oxigenDB_test$number").absolutePathString())
            .build()

        return ODatabaseConfig.builder()
            .withConnectionConfig(connConfig)
            .withDatabaseType(ODatabaseType.PLOCAL)
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
        db = initOrientDbServer(config.connectionConfig)
        provider = ODatabaseProviderImpl(config, db)
        provider.withSession { session ->
            session.createVertexClass("TEST")
        }
        logger.info("Set vertex property")
        provider.withSession { session ->
            session.executeInTx {
                val vertex = session.newVertex("TEST")
                vertex.setProperty("hello", "world")
                vertex.save<OVertex>()
            }
        }
        db.close()
        logger.info("Close the DB")
        Thread.sleep(1000)
        logger.info("Connect to db one more time and read")
        db = initOrientDbServer(config.connectionConfig)
        provider = ODatabaseProviderImpl(config, db)
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
        db = initOrientDbServer(config.connectionConfig)
        try {
            ODatabaseProviderImpl(noEncryptionConfig, db).apply {
                withSession { session ->
                    session.executeInTx {
                        val vertex = session.query("SELECT FROM TEST").vertexStream().toList()
                        print(vertex.size)
                    }
                }
                Assert.fail("Should not open")
            }
        } catch (_: OStorageException) {
            logger.info("As expected DB failed to initialize without key")
        } catch (e: AssertionError) {
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
            if (!Orient.instance().isActive) {
                Orient.instance().startup()
            }
        } catch (_: Throwable) {
            logger.error("CANNOT REINIT OXIGENDB")
        }
    }
}
