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
package jetbrains.exodus.entitystore.youtrackdb.iterate.property

import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence
import jetbrains.exodus.entitystore.Sequence
import jetbrains.exodus.entitystore.youtrackdb.YTDBPersistentEntityStore

internal class YTDBSequenceImpl(
    private val sequenceName: String,
    private val store: YTDBPersistentEntityStore
) : Sequence {
    override fun increment(): Long {
        return getOSequence().next()
    }

    override fun get(): Long {
        return getOSequence().current()
    }

    override fun set(l: Long) {
        val currentTx = store.requireActiveTransaction()
        currentTx.updateOSequence(sequenceName, l)
    }

    private fun getOSequence(): DBSequence {
        val currentTx = store.requireActiveTransaction()
        return currentTx.getOSequence(sequenceName)
    }
}
