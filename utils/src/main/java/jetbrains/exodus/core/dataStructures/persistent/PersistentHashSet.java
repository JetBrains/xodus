/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.core.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import org.jetbrains.annotations.NotNull;

public class PersistentHashSet<K> extends AbstractPersistentHashSet<K> {

    /**
     * The root of the last version of the tree.
     */
    @NotNull
    private volatile AbstractPersistentHashSet.RootTableNode<K> root;

    @SuppressWarnings("unchecked")
    public PersistentHashSet() {
        this(AbstractPersistentHashSet.EMPTY_ROOT);
    }

    public PersistentHashSet(@NotNull final AbstractPersistentHashSet.RootTableNode<K> root) {
        this.root = root;
    }

    public ImmutablePersistentHashSet<K> beginRead() {
        return new ImmutablePersistentHashSet<>(root);
    }

    public PersistentHashSet<K> getClone() {
        return new PersistentHashSet<>(root);
    }

    public MutablePersistentHashSet<K> beginWrite() {
        return new MutablePersistentHashSet<>(this);
    }

    boolean endWrite(MutablePersistentHashSet<K> tree) {
        if (root != tree.getStartingRoot()) {
            return false;
        }
        root = tree.getRoot();
        return true;
    }

    @NotNull
    AbstractPersistentHashSet.RootTableNode<K> getRoot() {
        return root;
    }

    public static class ImmutablePersistentHashSet<K> extends AbstractPersistentHashSet<K> {

        @NotNull
        private final RootTableNode<K> root;

        ImmutablePersistentHashSet(@NotNull final RootTableNode<K> root) {
            this.root = root;
        }

        @Override
        @NotNull
        RootTableNode<K> getRoot() {
            return root;
        }
    }

    public static class MutablePersistentHashSet<K> extends AbstractPersistentHashSet<K> implements Flag {

        private final PersistentHashSet<K> baseTree;
        private RootTableNode<K> startingRoot;
        private RootTableNode<K> root;
        private boolean flagged;

        MutablePersistentHashSet(@NotNull PersistentHashSet<K> tree) {
            startingRoot = tree.getRoot();
            root = startingRoot;
            baseTree = tree;
        }

        @Override
        public boolean flag() {
            return flagged = true;
        }

        public void add(@NotNull K key) {
            RootTableNode<K> actualRoot = root;
            flagged = false;
            Node<K> result = actualRoot.insert(key, key.hashCode(), 0, this);
            root = result.asRoot(flagged ? actualRoot.getSize() + 1 : actualRoot.getSize());
        }

        public boolean remove(@NotNull K key) {
            RootTableNode<K> actualRoot = root;
            @SuppressWarnings("unchecked") Node<K> result = (Node<K>) actualRoot.remove(key, key.hashCode(), 0);
            if (result != null) {
                root = result.asRoot(actualRoot.getSize() - 1);
                return true;
            }
            return false;
        }

        /**
         * Try to merge changes into the base tree.
         *
         * @return true if merging succeeded
         */
        public boolean endWrite() {
            if (baseTree.endWrite(this)) {
                startingRoot = root;
                return true;
            }
            return false;
        }

        @Override
        @NotNull
        RootTableNode<K> getRoot() {
            return root;
        }

        TableNode<K> getStartingRoot() {
            return startingRoot;
        }

        public void checkTip() {
            if (root != null) {
                root.checkNode(0);
            }
        }

        public void forEachKey(final ObjectProcedure<K> procedure) {
            root.forEachKey(procedure);
        }
    }
}
