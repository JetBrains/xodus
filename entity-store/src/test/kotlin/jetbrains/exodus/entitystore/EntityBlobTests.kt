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
package jetbrains.exodus.entitystore

import jetbrains.exodus.ExodusException
import jetbrains.exodus.TestFor
import jetbrains.exodus.TestUtil
import jetbrains.exodus.kotlin.notNull
import org.junit.Assert
import java.io.*

@Suppress("DEPRECATION")
class EntityBlobTests : EntityStoreTestBase() {

    override fun casesThatDontNeedExplicitTxn(): Array<String> {
        return arrayOf("testEntityStoreClear")
    }

    fun testInPlaceBlobs() {
        checkBlobs(storeTransaction)
    }

    fun testBlobs() {
        entityStore.config.maxInPlaceBlobSize = 0
        checkBlobs(storeTransaction)
    }

    fun testBlobFiles() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        Assert.assertNull(entity.getBlob("body"))
        entity.setBlob("body", createTempFile("my body"))
        entity.setBlob("body2", createTempFile("my body2"))
        Assert.assertEquals("my body", entity.getBlobString("body"))
        Assert.assertEquals("my body2", entity.getBlobString("body2"))
        txn.flush()
        Assert.assertEquals("my body", entity.getBlobString("body"))
        Assert.assertEquals("my body2", entity.getBlobString("body2"))
    }

    @TestFor(issue = "XD-675")
    fun testBlobFiles2() {
        entityStore.config.maxInPlaceBlobSize = 0
        testBlobFiles()
    }

    fun testDeleteBlobs() {
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        issue.setBlobString("description", "This is a test issue")
        issue.setBlob("body", createTempFile("my body"))
        txn.flush()
        Assert.assertEquals("This is a test issue", issue.getBlobString("description"))
        Assert.assertEquals("my body", issue.getBlobString("body"))
        issue.deleteBlob("description")
        issue.deleteBlob("body")
        txn.flush()
        Assert.assertNull(issue.getBlob("description"))
        Assert.assertNull(issue.getBlobString("description"))
        Assert.assertNull(issue.getBlob("body"))
        Assert.assertNull(issue.getBlobString("body"))
    }

    fun testDeleteBlobsWithinTxn() {
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        issue.setBlobString("description", "This is a test issue")
        issue.setBlob("body", createTempFile("my body"))
        Assert.assertEquals("This is a test issue", issue.getBlobString("description"))
        Assert.assertEquals("my body", issue.getBlobString("body"))
        issue.deleteBlob("description")
        issue.deleteBlob("body")
        Assert.assertNull(issue.getBlob("description"))
        Assert.assertNull(issue.getBlobString("description"))
        Assert.assertNull(issue.getBlob("body"))
        Assert.assertNull(issue.getBlobString("body"))
        issue.setBlobString("description", "This is a test issue")
        issue.setBlob("body", createTempFile("my body"))
        Assert.assertEquals("This is a test issue", issue.getBlobString("description"))
        Assert.assertEquals("my body", issue.getBlobString("body"))
        issue.deleteBlob("description")
        issue.deleteBlob("body")
        txn.flush()
        Assert.assertNull(issue.getBlob("description"))
        Assert.assertNull(issue.getBlobString("description"))
        Assert.assertNull(issue.getBlob("body"))
        Assert.assertNull(issue.getBlobString("body"))
    }

    fun testEmptyBlobString_XD_365() {
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        issue.setBlobString("description", "")
        Assert.assertNotNull(issue.getBlobString("description"))
        Assert.assertEquals("", issue.getBlobString("description"))
        txn.flush()
        Assert.assertNotNull(issue.getBlobString("description"))
        Assert.assertEquals("", issue.getBlobString("description"))
    }

    fun testNonAsciiBlobString() {
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        issue.setBlobString("description", "абвгдеёжзийклмнопрстуфхкцчшщъыьэюя")
        txn.flush()
        Assert.assertEquals("абвгдеёжзийклмнопрстуфхкцчшщъыьэюя", issue.getBlobString("description"))
    }

    @TestFor(issue = "JT-44824")
    fun testLargeBlobString() {
        val builder = StringBuilder()
        val blobStringSize = 80000
        for (i in 0 until blobStringSize) {
            builder.append(' ')
        }
        val txn = storeTransaction
        val issue = txn.newEntity("Issue")
        issue.setBlobString("blank", builder.toString())
        txn.flush()
        val blank = issue.getBlobString("blank").notNull
        for (i in 0 until blobStringSize) {
            Assert.assertEquals(' '.toLong(), blank[i].toLong())
        }
    }

    @TestFor(issue = "XD-362")
    fun testXD_362() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        Assert.assertNull(entity.getBlob("body"))
        val data = this.javaClass.classLoader.getResource("testXD_362.data").notNull
        entity.setBlob("body", data.openStream())
        Assert.assertTrue(entity.getBlobSize("body") > 0L)
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), data.openStream(), false))
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), data.openStream(), false))
        txn.flush()
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), data.openStream()))
    }

    fun testBlobOverwrite() {
        val txn = storeTransaction
        txn.flush()
        val entity = txn.newEntity("Issue")
        Assert.assertNull(entity.getBlob("body"))
        entity.setBlob("body", string2Stream("body"))
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body")))
        entity.setBlob("body", string2Stream("body1"))
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body1")))
        txn.flush()
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body1")))
        entity.setBlob("body", string2Stream("body2"))
        txn.flush()
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body2")))
    }

    fun testSingleNameBlobAndProperty() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        Assert.assertNull(entity.getBlob("body"))
        entity.setBlob("body", string2Stream("stream body"))
        entity.setProperty("body", "string body")
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("stream body")))
        txn.flush()
        Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("stream body")))
        Assert.assertEquals(entity.getProperty("body"), "string body")
    }

    fun testMultipleBlobs() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        for (i in 0..1999) {
            entity.setBlob("body$i", string2Stream("body$i"))
        }
        txn.flush()
        for (i in 0..1999) {
            Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body$i"), string2Stream("body$i")))
        }
    }

    fun testConcurrentMultipleBlobs() {
        val txn = storeTransaction
        val entity = txn.newEntity("Issue")
        for (i in 0..1999) {
            entity.setBlob("body$i", string2Stream("body$i"))
        }
        txn.flush()
        val wereExceptions = booleanArrayOf(false)
        val threads = arrayOfNulls<Thread>(10)
        for (t in threads.indices) {
            val thread = Thread {
                val txn1 = entityStore.beginTransaction()
                for (i in 0..1999) {
                    try {
                        if (!TestUtil.streamsEqual(entity.getBlob("body$i"), string2Stream("body$i"))) {
                            wereExceptions[0] = true
                            break
                        }
                    } catch (e: Exception) {
                        wereExceptions[0] = true
                        break
                    }
                }
                txn1.commit()
            }
            thread.start()
            threads[t] = thread
        }
        for (thread in threads) {
            thread?.join()
        }
        Assert.assertFalse(wereExceptions[0])
    }

    @TestFor(issue = "XD-531")
    fun testEntityStoreClear() {
        val store = entityStore
        store.config.maxInPlaceBlobSize = 0
        store.executeInTransaction { txn ->
            txn.newEntity("User").setBlobString("bio", "I was born")
        }
        store.executeInReadonlyTransaction { txn ->
            txn as PersistentStoreTransaction
            val content = store.blobVault.getContent(0, txn.environmentTransaction)
            assertNotNull(content)
            try {
                content?.close()
            } catch (e: IOException) {
                throw ExodusException.toExodusException(e)
            }
        }
        store.clear()
        store.executeInReadonlyTransaction { txn ->
            assertNull(store.blobVault
                .getContent(0, (txn as PersistentStoreTransaction).environmentTransaction))
        }
    }

    @TestFor(issues = ["JT-63580", "JT-63582"])
    fun testDeleteDuplicate() {
        val store = entityStore
        // enable v2 format
        store.environment.environmentConfig.useVersion1Format = false
        for (i in 0..2) {
            store.executeInTransaction { txn ->
                txn.newEntity("Issue").setBlobString("description", "This is test description")
            }
        }
        PersistentEntityStoreRefactorings(store).refactorDeduplicateInPlaceBlobs();
        store.executeInReadonlyTransaction { txn ->
            txn.getAll("Issue").forEach { entity ->
                Assert.assertEquals("This is test description", entity.getBlobString("description"))
            }
        }
        store.executeInTransaction { txn -> txn.getAll("Issue").first?.delete() }
    }
}

private fun checkBlobs(txn: StoreTransaction) {
    val entity = txn.newEntity("Issue")
    Assert.assertNull(entity.getBlob("body"))
    Assert.assertEquals(-1L, entity.getBlobSize("body"))
    val length = "body".toByteArray().size
    entity.setBlob("body", string2Stream("body"))
    entity.setBlob("body2", createTempFile("body"))
    Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body")))
    Assert.assertEquals(length.toLong(), entity.getBlobSize("body"))
    Assert.assertEquals("body", entity.getBlobString("body2"))
    Assert.assertEquals((length + 2).toLong(), entity.getBlobSize("body2"))
    txn.flush()
    Assert.assertTrue(TestUtil.streamsEqual(entity.getBlob("body"), string2Stream("body")))
    Assert.assertEquals(length.toLong(), entity.getBlobSize("body"))
    Assert.assertEquals("body", entity.getBlobString("body2"))
    Assert.assertEquals((length + 2).toLong(), entity.getBlobSize("body2"))
}

private fun createTempFile(content: String): File {
    val tempFile = File.createTempFile("test", null)
    val output = DataOutputStream(FileOutputStream(tempFile))
    output.writeUTF(content)
    output.close()
    return tempFile
}

private fun string2Stream(s: String): InputStream {
    return ByteArrayInputStream(s.toByteArray())
}
