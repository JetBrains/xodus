/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.tree.patricia

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.ITreeCursor
import jetbrains.exodus.tree.ITreeMutable

internal class PatriciaTreeEmpty(log: Log, structureId: Int, hasDuplicates: Boolean) :
    PatriciaTreeBase(log, structureId) {
    private val hasDuplicates: Boolean

    init {
        size = 0
        this.hasDuplicates = hasDuplicates
    }

    override fun getMutableCopy(): ITreeMutable {
        val treeMutable = PatriciaTreeMutable(
            log, structureId, 0,
            (getRoot() as ImmutableNode)
        )
        return if (hasDuplicates) PatriciaTreeWithDuplicatesMutable(treeMutable) else treeMutable
    }

    override fun getRootAddress(): Long = Loggable.NULL_ADDRESS

    override fun openCursor(): ITreeCursor {
        return ITreeCursor.EMPTY_CURSOR
    }

    override fun getRoot(): NodeBase = SinglePageImmutableNode()

    override fun getNode(key: ByteIterable): NodeBase? {
        return null
    }
}
