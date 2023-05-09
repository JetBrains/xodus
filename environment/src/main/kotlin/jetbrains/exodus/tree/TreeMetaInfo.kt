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
package jetbrains.exodus.tree

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.log.*
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.fillBytes
import jetbrains.exodus.tree.btree.BTreeMetaInfo
import jetbrains.exodus.tree.patricia.PatriciaMetaInfo
import jetbrains.exodus.util.LightOutputStream

abstract class TreeMetaInfo protected constructor(val log: Log?, val duplicates: Boolean, val structureId: Int) {
    fun hasDuplicates(): Boolean {
        return duplicates
    }

    abstract fun isKeyPrefixing(): Boolean
    fun toByteIterable(): ByteIterable {
        var flags = (if (duplicates) DUPLICATES_BIT else 0).toByte()
        if (isKeyPrefixing()) {
            flags = (flags + KEY_PREFIXING_BIT.toByte()).toByte()
        }
        val output = LightOutputStream(10)
        output.write(flags.toInt())
        fillBytes(0, output) // legacy format
        fillBytes(structureId.toLong(), output)
        return output.asArrayByteIterable()
    }

    abstract fun clone(newStructureId: Int): TreeMetaInfo
    private class Empty(structureId: Int) : TreeMetaInfo(null, false, structureId) {
        override fun isKeyPrefixing(): Boolean = false

        override fun clone(newStructureId: Int): TreeMetaInfo {
            return Empty(newStructureId)
        }
    }

    companion object {
        val EMPTY: TreeMetaInfo = Empty(0)
        const val DUPLICATES_BIT = 1
        protected const val KEY_PREFIXING_BIT = 2
        fun toConfig(metaInfo: TreeMetaInfo): StoreConfig {
            return if (metaInfo.structureId < 0) {
                StoreConfig.TEMPORARY_EMPTY
            } else StoreConfig.getStoreConfig(metaInfo.duplicates, metaInfo.isKeyPrefixing())
        }

        fun load(
            environment: EnvironmentImpl,
            duplicates: Boolean,
            keyPrefixing: Boolean,
            structureId: Int
        ): TreeMetaInfo {
            return if (keyPrefixing) {
                PatriciaMetaInfo(environment.log, duplicates, structureId)
            } else {
                BTreeMetaInfo(environment, duplicates, structureId)
            }
        }

        fun load(environment: EnvironmentImpl, iterable: ByteIterable): TreeMetaInfo {
            val it = iterable.iterator()
            val flagsByte = it.next()
            return if (flagsByte.toInt() and KEY_PREFIXING_BIT == 0) {
                BTreeMetaInfo.load(environment, flagsByte, it)
            } else {
                PatriciaMetaInfo.load(environment, flagsByte, it)
            }
        }

        fun getTreeLoggables(tree: ITree): ExpiredLoggableCollection {
            val log = tree.getLog()
            val result: ExpiredLoggableCollection = ExpiredLoggableCollection.newInstance(log)
            val it = tree.addressIterator()
            while (it.hasNext()) {
                val nextAddress = it.next()
                result.add(log.readNotNull(tree.getDataIterator(nextAddress), nextAddress))
            }
            return result
        }
    }
}
