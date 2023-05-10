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

internal open class PatriciaTree(log: Log, rootAddress: Long, structureId: Int) : PatriciaTreeBase(log, structureId) {
    private val rootLoggable: RandomAccessLoggable

    private val root: ImmutableNode
    override fun getRoot(): NodeBase = root.asNodeBase()

    init {
        require(rootAddress != Loggable.NULL_ADDRESS) { "Can't instantiate nonempty tree with null root address" }
        rootLoggable = getLoggable(rootAddress)
        val type = rootLoggable.getType()
        if (!nodeIsRoot(type)) {
            throw ExodusException("Unexpected root page type: $type")
        }
        val data = rootLoggable.getData()
        val it = data.iterator()
        size = it.getCompressedUnsignedLong()
        if (nodeHasBackReference(type)) {
            val backRef = it.getCompressedUnsignedLong()
            @Suppress("LeakingThis")
            rememberBackRef(backRef)
        }

        root = if (rootLoggable.isDataInsideSinglePage()) {
            SinglePageImmutableNode(
                rootLoggable,
                data.cloneWithAddressAndLength(it.getAddress(), it.available())
            )
        } else {
            MultiPageImmutableNode(
                log, rootLoggable,
                data.cloneWithAddressAndLength(it.getAddress(), it.available())
            )
        }
    }

    override fun getMutableCopy(): PatriciaTreeMutable = PatriciaTreeMutable(log, structureId, size, root)
    override fun getRootAddress(): Long = rootLoggable.getAddress()

    override fun openCursor(): ITreeCursor {
        return TreeCursor(PatriciaTraverser(this, root.asNodeBase()), root.hasValue())
    }

    open fun rememberBackRef(backRef: Long) {
        // do nothing
    }
}
