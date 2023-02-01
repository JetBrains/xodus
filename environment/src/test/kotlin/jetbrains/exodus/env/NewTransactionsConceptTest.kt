package jetbrains.exodus.env

import jetbrains.exodus.*
import jetbrains.exodus.bindings.StringBinding
import org.junit.Assert.*
import org.junit.Test
import java.util.*
import kotlin.math.abs

class NewTransactionsConceptTest : EnvironmentTestsBase() {

    @Test
    fun testPut() {
        putTest(StoreConfig.WITHOUT_DUPLICATES)
    }

    @Test
    fun testTruncate() {
        truncateTest(StoreConfig.WITHOUT_DUPLICATES)
    }

    private fun putTest(config: StoreConfig) {
        val env = environment
        val txn: Transaction = env.beginTransaction()
        val store = env.openStore("store", config, txn)
        assertTrue(store.put(txn, key, value))
        txn.commit()
        assertNotNullStringValue(store, key, "value")
    }


    private fun truncateTest(config: StoreConfig) {
        var txn: Transaction = env.beginTransaction()
        val store = env.openStore("store", config, txn)
        store.put(txn, key, value)
        txn.commit()
        assertNotNullStringValue(store, key, "value")
        txn = env.beginTransaction()
        env.truncateStore("store", txn)
        txn.commit()
        assertEmptyValue(store, key)
        openStoreAutoCommit("store", StoreConfig.USE_EXISTING)
        assertEmptyValue(store, key)
    }


    companion object {
        private val key: ByteIterable
            get() = StringBinding.stringToEntry("key")

        private val value: ByteIterable
            get() = StringBinding.stringToEntry("value")

        private val rnd = Random(77634963005211L)

        private fun randomLong(): Long {
            return abs(rnd.nextLong())
        }
    }
}
