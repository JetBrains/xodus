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
package jetbrains.exodus.tree.btree

import jetbrains.exodus.ByteIterator
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getInt
import jetbrains.exodus.log.Log
import jetbrains.exodus.tree.TreeMetaInfo

class BTreeMetaInfo private constructor(
    log: Log,
    private val balancePolicy: BTreeBalancePolicy,
    duplicates: Boolean,
    structureId: Int
) : TreeMetaInfo(log, duplicates, structureId) {
    constructor(env: EnvironmentImpl, duplicates: Boolean, structureId: Int) : this(
        env.log,
        env.getBTreeBalancePolicy(),
        duplicates,
        structureId
    )

    override fun isKeyPrefixing(): Boolean = false

    override fun clone(newStructureId: Int): BTreeMetaInfo {
        return BTreeMetaInfo(log!!, balancePolicy, duplicates, newStructureId)
    }

    companion object {
        fun load(env: EnvironmentImpl, flagsByte: Byte, it: ByteIterator?): BTreeMetaInfo {
            val duplicates = flagsByte.toInt() and DUPLICATES_BIT != 0
            getInt(it!!) // legacy format
            val structureId = getInt(it)
            return BTreeMetaInfo(env, duplicates, structureId)
        }
    }
}
