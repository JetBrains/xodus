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
package jetbrains.exodus.core.dataStructures.persistent

import jetbrains.exodus.core.dataStructures.hash.*

class PersistentHashSet<K> @JvmOverloads constructor(
    /**
     * The root of the last version of the tree.
     */
    @field:Volatile override var root: RootTableNode<K> = AbstractPersistentHashSet.Companion.EMPTY_ROOT
) : AbstractPersistentHashSet<K>() {
    fun beginRead(): ImmutablePersistentHashSet<K> {
        return ImmutablePersistentHashSet(root)
    }

    val clone: PersistentHashSet<K>
        get() = PersistentHashSet(root)

    fun beginWrite(): MutablePersistentHashSet<K> {
        return MutablePersistentHashSet(this)
    }

    fun endWrite(tree: MutablePersistentHashSet<K>): Boolean {
        if (root !== tree.getStartingRoot()) {
            return false
        }
        root = tree.root
        return true
    }

    open class ImmutablePersistentHashSet<K> internal constructor(root: RootTableNode<K?>) :
        AbstractPersistentHashSet<K>() {
        override val root: RootTableNode<K>

        init {
            this.root = root
        }
    }

    open class MutablePersistentHashSet<K> internal constructor(tree: PersistentHashSet<K?>) :
        AbstractPersistentHashSet<K?>(), Flag {
        private val baseTree: PersistentHashSet<K?>
        private var startingRoot: RootTableNode<K?>
        override var root: RootTableNode<K?>
            private set
        private var flagged = false

        init {
            startingRoot = tree.root
            root = startingRoot
            baseTree = tree
        }

        override fun flag(): Boolean {
            return true.also { flagged = it }
        }

        fun add(key: K) {
            val actualRoot = root
            flagged = false
            val result: Node<K> = actualRoot.insert(key, key.hashCode(), 0, this)
            root = result.asRoot(if (flagged) actualRoot.size + 1 else actualRoot.size)
        }

        fun remove(key: K): Boolean {
            val actualRoot = root
            val result = actualRoot.remove(key, key.hashCode(), 0) as Node<K>?
            if (result != null) {
                root = result.asRoot(actualRoot.size - 1)
                return true
            }
            return false
        }

        /**
         * Try to merge changes into the base tree.
         *
         * @return true if merging succeeded
         */
        fun endWrite(): Boolean {
            if (baseTree.endWrite(this)) {
                startingRoot = root
                return true
            }
            return false
        }

        fun getStartingRoot(): TableNode<K?> {
            return startingRoot
        }

        fun checkTip() {
            if (root != null) {
                root.checkNode(0)
            }
        }

        fun forEachKey(procedure: ObjectProcedure<K?>?) {
            root.forEachKey(procedure!!)
        }
    }
}
