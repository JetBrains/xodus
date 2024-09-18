package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.orientdb.testutil.InMemoryOrientDB
import jetbrains.exodus.entitystore.orientdb.testutil.OTestMixin
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertFailsWith

class ODatabaseProviderTest: OTestMixin {

    @Rule
    @JvmField
    val orientDbRule = InMemoryOrientDB()

    override val orientDb = orientDbRule

    @Test
    fun `read-only mode works`() {
        // by default it is read-write
        withSession { session ->
            val v = session.newVertex()
            v.save<OVertex>()
        }

        orientDb.provider.readOnly = true
        withSession { session ->
            val v = session.newVertex()
            assertFailsWith<OModificationOperationProhibitedException> { v.save<OVertex>() }
        }

        orientDb.provider.readOnly = false
        withSession { session ->
            val v = session.newVertex()
            v.save<OVertex>()
        }

        orientDb.provider.readOnly = true
        // close() releases the read-only mode before closing the database (otherwise it throws exceptions)
        orientDb.provider.close()
    }

}