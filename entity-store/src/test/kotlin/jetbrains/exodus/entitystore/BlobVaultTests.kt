package jetbrains.exodus.entitystore

import jetbrains.exodus.TestFor
import jetbrains.exodus.backup.BackupStrategy
import org.junit.Assert
import java.io.ByteArrayInputStream
import java.io.File
import java.util.*

class BlobVaultTests : EntityStoreTestBase() {

    fun testNewVaultFilesLocality() {
        val store = entityStore
        val txn = storeTransaction
        store.config.maxInPlaceBlobSize = 0 // no in-lace blobs
        for (i in 0..255) {
            txn.newEntity("E").setBlob("b", ByteArrayInputStream("content".toByteArray()))
        }
        Assert.assertTrue(txn.flush())
        val blobVault = store.blobVault.sourceVault as FileSystemBlobVaultOld
        val vaultLocation = blobVault.vaultLocation
        Assert.assertEquals(257, vaultLocation.listFiles()!!.size.toLong()) // + "version" file
        Assert.assertEquals(256, vaultLocation.listFiles { _, name -> name.endsWith(PersistentEntityStoreImpl.BLOBS_EXTENSION) }!!.size.toLong())
        for (i in 0..255) {
            txn.newEntity("E").setBlob("b", ByteArrayInputStream("content".toByteArray()))
        }
        Assert.assertTrue(txn.flush())
        Assert.assertEquals(258, vaultLocation.listFiles()!!.size.toLong())
        Assert.assertEquals(256, vaultLocation.listFiles { _, name -> name.endsWith(PersistentEntityStoreImpl.BLOBS_EXTENSION) }!!.size.toLong())
    }

    fun testHandleToFileAndFileToHandle() {
        val store = entityStore
        val txn = storeTransaction
        store.config.maxInPlaceBlobSize = 0 // no in-lace blobs
        val count = 1000
        for (i in 0 until count) {
            txn.newEntity("E").setBlob("b", ByteArrayInputStream("content".toByteArray()))
        }
        Assert.assertTrue(txn.flush())
        val blobVault = store.blobVault.sourceVault as FileSystemBlobVault
        val handlesToFiles = TreeMap<Long, File>()
        for (fd in blobVault.backupStrategy.contents) {
            val file = (fd as BackupStrategy.FileDescriptor).file
            if (file.isFile && file.name != FileSystemBlobVaultOld.VERSION_FILE) {
                val handle = blobVault.getBlobHandleByFile(file)
                Assert.assertFalse(handlesToFiles.containsKey(handle))
                handlesToFiles[handle] = file
                Assert.assertEquals(file, blobVault.getBlobLocation(handle))
            }
        }
        val min = handlesToFiles.navigableKeySet().iterator().next()
        Assert.assertEquals(0L, min)
        val max = handlesToFiles.descendingKeySet().iterator().next()
        Assert.assertEquals((count - 1).toLong(), max)
    }

    @TestFor(issues = ["XD-679"])
    fun testSaveReadStream() {
        val txn = storeTransaction
        val store = entityStore
        store.config.maxInPlaceBlobSize = 0
        val e = txn.newEntity("E")
        e.setBlobString("c", "content")
        Assert.assertEquals("content", e.getBlobString("c"))
        txn.flush()
        store.executeInReadonlyTransaction { Assert.assertEquals("content", e.getBlobString("c")) }
    }
}
