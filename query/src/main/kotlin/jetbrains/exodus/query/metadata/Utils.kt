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

import com.jetbrains.youtrack.db.api.DatabaseSession
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.StoreTransaction

fun <R> PersistentEntityStore.withReadonlyTx(block: (StoreTransaction) -> R): R {
    val tx = this.beginReadonlyTransaction()
    try {
        val result = block(tx)
        tx.abort()
        return result
    } catch(e: Throwable) {
        if (!tx.isFinished) {
            tx.abort()
        }
        throw e
    }
}

fun <R> DatabaseSession.withTx(block: (DatabaseSession) -> R): R {
    this.begin()
    try {
        val result = block(this)
        this.commit()
        return result
    } catch(e: Throwable) {
        this.rollback()
        throw e
    }
}