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

import jetbrains.exodus.ExodusException
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.RandomAccessLoggable
import jetbrains.exodus.tree.ITreeCursor
import jetbrains.exodus.tree.TreeCursor

open class PatriciaTree(log: Log, rootAddress: Long, structureId: Int) : PatriciaTreeBase(log, structureId) {
    private val rootLoggable: RandomAccessLoggable

    private val _root: ImmutableNode
    override val root: NodeBase
        get() = _root.asNodeBase()

    init {
        require(rootAddress != Loggable.NULL_ADDRESS) { "Can't instantiate nonempty tree with null root address" }
        rootLoggable = getLoggable(rootAddress)
        val type = rootLoggable.type
        if (!nodeIsRoot(type)) {
            throw ExodusException("Unexpected root page type: $type")
        }
        val data = rootLoggable.data
        val it = data.iterator()
        size = it.compressedUnsignedLong
        if (nodeHasBackReference(type)) {
            val backRef = it.compressedUnsignedLong
            @Suppress("LeakingThis")
            rememberBackRef(backRef)
        }

        _root = if (rootLoggable.isDataInsideSinglePage) {
            SinglePageImmutableNode(
                rootLoggable,
                data.cloneWithAddressAndLength(it.address, it.available())
            )
        } else {
            MultiPageImmutableNode(
                log, rootLoggable,
                data.cloneWithAddressAndLength(it.address, it.available())
            )
        }
    }

    override val mutableCopy: PatriciaTreeMutable
        get() = PatriciaTreeMutable(log, structureId, size, _root)
    override val rootAddress: Long
        get() = rootLoggable.address

    override fun openCursor(): ITreeCursor {
        return TreeCursor(PatriciaTraverser(this, root), root.hasValue())
    }

    open fun rememberBackRef(backRef: Long) {
        // do nothing
    }
}
