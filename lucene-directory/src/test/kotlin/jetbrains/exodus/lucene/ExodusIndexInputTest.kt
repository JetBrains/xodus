package jetbrains.exodus.lucene

import org.apache.lucene.store.IOContext
import org.junit.Assert
import org.junit.Test

class ExodusIndexInputTest : ExodusLuceneWithPatriciaTests() {

    @Test
    fun clonedFilePointer() {
        addSingleDocument()
        closeIndexWriter()
        txn?.flush()
        val nonEmptyFile = directory.listAll().first { directory.fileLength(it) > 0 }
        val input = directory.openInput(nonEmptyFile, IOContext.DEFAULT)
        Assert.assertEquals(0L, input.filePointer)
        Assert.assertEquals(0L, input.clone().filePointer)
        input.readByte()
        Assert.assertEquals(1L, input.filePointer)
        Assert.assertEquals(1L, input.clone().filePointer)
    }
}