package jetbrains.exodus.entitystore.orientdb.testutil

import com.google.common.truth.Truth.assertThat
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OStoreTransactionImpl

interface OTestMixin {

    val orientDb: InMemoryOrientDB

    fun assertNamesExactlyInOrder(result: Iterable<Entity>, vararg names: String) {
        assertThat(result.map { it.getProperty("name") }).containsExactly(*names).inOrder()
    }

    fun beginTransaction(): OStoreTransactionImpl {
        val store = orientDb.store
        return store.beginTransaction() as OStoreTransactionImpl
    }

    fun oTransactional(block: (StoreTransaction) -> Unit) {
        orientDb.store.executeInTransaction {
            block(it)
        }
    }

    fun givenTestCase() = OTaskTrackerTestCase(orientDb)
}