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

import jetbrains.exodus.core.dataStructures.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

public class PersistentLong23TreeMap<V> implements PersistentLongMap<V> {

    private final Persistent23Tree<PersistentLongMap.Entry<V>> set;

    public PersistentLong23TreeMap() {
        this(null);
    }

    private PersistentLong23TreeMap(@Nullable final AbstractPersistent23Tree.RootNode<PersistentLongMap.Entry<V>> root) {
        set = new Persistent23Tree<>(root);
    }

    @Override
    public PersistentLongMap.ImmutableMap<V> beginRead() {
        return new ImmutableMap<>(set.getRoot());
    }

    @Override
    public PersistentLong23TreeMap<V> getClone() {
        return new PersistentLong23TreeMap<>(set.getRoot());
    }

    @Override
    public PersistentLongMap.MutableMap<V> beginWrite() {
        return new MutableMap<>(set);
    }

    @Deprecated
    public boolean endWrite(MutableMap<V> tree) {
        return set.endWrite(tree);
    }

    @SuppressWarnings("unchecked")
    protected static class ImmutableMap<V> extends Persistent23Tree.ImmutableTree<PersistentLongMap.Entry<V>> implements PersistentLongMap.ImmutableMap<V> {

        ImmutableMap(RootNode<PersistentLongMap.Entry<V>> root) {
            super(root);
        }

        @Override
        public V get(long key) {
            Node<PersistentLongMap.Entry<V>> root = getRoot();
            if (root == null) {
                return null;
            }
            PersistentLongMap.Entry<V> entry = root.getByWeight(key);
            return entry == null ? null : entry.getValue();
        }

        @Override
        public boolean containsKey(long key) {
            final Node<PersistentLongMap.Entry<V>> root = getRoot();
            return root != null && root.getByWeight(key) != null;
        }

        @Override
        public Iterator<Entry<V>> tailEntryIterator(long staringKey) {
            return tailIterator(new LongMapEntry<V>(staringKey));
        }

        @Override
        public Iterator<Entry<V>> tailReverseEntryIterator(long staringKey) {
            return tailReverseIterator(new LongMapEntry<V>(staringKey));
        }
    }

    @SuppressWarnings("unchecked")
    protected static class MutableMap<V> extends Persistent23Tree.MutableTree<PersistentLongMap.Entry<V>> implements PersistentLongMap.MutableMap<V>, RootHolder {

        MutableMap(Persistent23Tree<PersistentLongMap.Entry<V>> set) {
            super(set);
        }

        @Override
        public V get(long key) {
            Node<PersistentLongMap.Entry<V>> root = getRoot();
            if (root == null) {
                return null;
            }
            PersistentLongMap.Entry<V> entry = root.getByWeight(key);
            return entry == null ? null : entry.getValue();
        }

        @Override
        public boolean containsKey(long key) {
            return get(key) != null;
        }

        @Override
        public Iterator<Entry<V>> tailEntryIterator(long staringKey) {
            return tailIterator(new LongMapEntry<V>(staringKey));
        }

        @Override
        public Iterator<Entry<V>> tailReverseEntryIterator(long staringKey) {
            return tailReverseIterator(new LongMapEntry<V>(staringKey));
        }

        @Override
        public void put(long key, @NotNull V value) {
            add(new LongMapEntry<>(key, value));
        }

        @Override
        public V remove(long key) {
            RootNode<PersistentLongMap.Entry<V>> root = getRoot();
            if (root == null) {
                return null;
            }
            Pair<Node<PersistentLongMap.Entry<V>>, PersistentLongMap.Entry<V>> removeResult = root.remove(new LongMapEntry<V>(key), true);
            if (removeResult == null) {
                return null;
            }
            Node<PersistentLongMap.Entry<V>> res = removeResult.getFirst();
            if (res instanceof RemovedNode) {
                res = res.getFirstChild();
            }
            root = res == null ? null : res.asRoot(root.getSize() - 1);
            setRoot(root);
            return removeResult.getSecond().getValue();
        }
    }
}
