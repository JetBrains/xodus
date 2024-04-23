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

import com.orientechnologies.orient.core.db.ODatabaseSession


/**
 * A class that provides functionality for commiting a transaction every X changes.
 *
 * For example, you copy 1B entities, you do not want to copy them in the scope of a single transaction,
 * because when the transaction fails at the last entity, you have to start copying from the beginning.
 *
 * If you use a counting transaction and set it commit every 100 entities. You just copy entity by entity,
 * increment() the transaction, and it will make sure to commit when necessary.
 *
 * @property oSession The underlying ODatabaseSession that this CountingOTransaction operates on.
 * @property commitEvery The number of changes increments before a commit is triggered.
 */
class CountingOTransaction(
    private val oSession: ODatabaseSession,
    private val commitEvery: Int
) {
    private var counter = 0

    fun increment() {
        counter++
        if (counter == commitEvery) {
            oSession.commit()
            oSession.begin()
            counter = 0
        }
    }

    fun begin() {
        require(oSession.transaction == null || !oSession.transaction.isActive)
        oSession.begin()
    }

    fun commit() {
        oSession.transaction?.let { tx ->
            if (tx.isActive) {
                oSession.commit()
            }
        }
    }

    fun rollback() {
        oSession.transaction?.let { tx ->
            if (tx.isActive) {
                oSession.rollback()
            }
        }
    }
}

fun <R> ODatabaseSession.withCountingTx(commitEvery: Int, block: (CountingOTransaction) -> R): R {
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