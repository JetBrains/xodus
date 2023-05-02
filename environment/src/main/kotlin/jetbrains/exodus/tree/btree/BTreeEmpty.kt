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

import jetbrains.exodus.*
import jetbrains.exodus.log.*
import jetbrains.exodus.tree.ITreeCursor

open class BTreeEmpty(
    log: Log,
    balancePolicy: BTreeBalancePolicy,
    allowsDuplicates: Boolean,
    structureId: Int
) : BTreeBase(log, balancePolicy, allowsDuplicates, structureId) {
    init {
        size = 0
    }

    constructor(log: Log, allowsDuplicates: Boolean, structureId: Int) : this(
        log,
        BTreeBalancePolicy.DEFAULT,
        allowsDuplicates,
        structureId
    )

    override val mutableCopy: BTreeMutable
        get() = BTreeMutable(this)
    override val rootAddress: Long
        get() = Loggable.NULL_ADDRESS
    override val isEmpty: Boolean
        get() = true

    override fun openCursor(): ITreeCursor {
        return ITreeCursor.EMPTY_CURSOR
    }

    override fun hasKey(key: ByteIterable): Boolean {
        return false
    }

    override fun hasPair(key: ByteIterable, value: ByteIterable): Boolean {
        return false
    }

    override val root: BasePage
        get() = BottomPage(this)
}
