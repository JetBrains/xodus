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
package jetbrains.exodus.query.metadata

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction


/**
 * A class that provides functionality for commiting a transaction every X changes.
 *
 * For example, you copy 1B entities, you do not want to copy them in the scope of a single transaction,
 * because when the transaction fails at the last entity, you have to start copying from the beginning.
 *
 * If you use a counting transaction and set it, commit every 100 entities. You just copy entity by entity,
 * increment() the transaction, and it will make sure to commit when necessary.
 *
 * @property commitEvery The number of changes increments before a commit is triggered.
 */
class CountingOTransaction(
    private val store: OPersistentEntityStore,
    private val commitEvery: Int
) {
    private var counter = 0
    private lateinit var txn : StoreTransaction

    var transactionsCommited: Long = 0
        private set

    fun increment() {
        counter++
        if (counter == commitEvery) {
            txn.flush()
            transactionsCommited++
            counter = 0
        }
    }

    fun begin() {
        txn = store.beginTransaction()
    }

    fun commit() {
        if (!txn.isFinished) {
            txn.commit()
            transactionsCommited++
        }
    }

    fun rollback() {
        txn.abort()
    }

    val session get() = (txn as OStoreTransaction).activeSession

}

fun <R> OPersistentEntityStore.withCountingTx(commitEvery: Int, block: (CountingOTransaction) -> R): R {
    val tx = CountingOTransaction(this, commitEvery)
    tx.begin()
    try {
        val result = block(tx)
        tx.commit()
        return result
    } catch(e: Throwable) {
        tx.rollback()
        throw e
    }
}
