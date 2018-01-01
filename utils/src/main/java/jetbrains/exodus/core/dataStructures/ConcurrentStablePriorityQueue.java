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
package jetbrains.exodus.core.dataStructures;

import jetbrains.exodus.core.dataStructures.persistent.Persistent23Tree;
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet;
import jetbrains.exodus.core.execution.locks.Guard;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentStablePriorityQueue<P extends Comparable<? super P>, E> extends PriorityQueue<P, E> {

    private final AtomicReference<
            Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>>> rootPair;

    public ConcurrentStablePriorityQueue() {
        rootPair = new AtomicReference<>();
    }

    @Override
    public boolean isEmpty() {
        final Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> currentPair = getCurrent();
        return currentPair == null || currentPair.getFirst().isEmpty();
    }

    @Override
    public int size() {
        final Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> currentPair = getCurrent();
        return currentPair == null ? 0 : currentPair.getFirst().size();
    }

    @Override
    @SuppressWarnings({"ObjectAllocationInLoop", "OverlyLongMethod"})
    public E push(@NotNull final P priority, @NotNull final E value) {
        Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> currentPair;
        Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> newPair;
        E result;

        do {
            result = null;
            currentPair = getCurrent();
            final Persistent23Tree<TreeNode<P, E>> queue;
            final PersistentHashSet<IdentifiedTreeNode<P, E>> values;
            final Persistent23Tree.MutableTree<TreeNode<P, E>> mutableQueue;
            final PersistentHashSet.MutablePersistentHashSet<IdentifiedTreeNode<P, E>> mutableValues;

            final TreeNode<P, E> node = new TreeNode<>(priority, value);
            final IdentifiedTreeNode<P, E> idNode = new IdentifiedTreeNode<>(node);

            if (currentPair == null) {
                queue = new Persistent23Tree<>();
                values = new PersistentHashSet<>();
                mutableQueue = queue.beginWrite();
                mutableValues = values.beginWrite();
            } else {
                queue = currentPair.getFirst().getClone();
                values = currentPair.getSecond().getClone();
                mutableQueue = queue.beginWrite();
                mutableValues = values.beginWrite();
                final IdentifiedTreeNode<P, E> oldIdNode = mutableValues.getKey(idNode);
                if (oldIdNode != null) {
                    final TreeNode<P, E> oldNode = oldIdNode.node;
                    result = oldNode.value;
                    mutableValues.remove(oldIdNode);
                    mutableQueue.exclude(oldNode);
                }
            }
            mutableQueue.add(node);
            mutableValues.add(idNode);
            // commit trees and then try to commit pair of trees
            // no need to check endWrite() results since they commit cloned trees
            mutableQueue.endWrite();
            mutableValues.endWrite();
            newPair = new Pair<>(queue, values);
            // commit pair if no other pair was already committed
        } while (!rootPair.compareAndSet(currentPair, newPair));

        return result;
    }

    @Override
    public Pair<P, E> peekPair() {
        final Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> currentPair = getCurrent();
        if (currentPair == null) {
            return null;
        }
        final TreeNode<P, E> max = currentPair.getFirst().getMaximum();
        return max == null ? null : new Pair<>(max.priority, max.value);
    }

    @Nullable
    @Override
    public Pair<P, E> floorPair() {
        final Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> currentPair = getCurrent();
        if (currentPair == null) {
            return null;
        }
        final TreeNode<P, E> min = currentPair.getFirst().getMinimum();
        return min == null ? null : new Pair<>(min.priority, min.value);
    }

    @Override
    @SuppressWarnings("ObjectAllocationInLoop")
    public E pop() {
        Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> currentPair;
        Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> newPair;
        E result;

        do {
            result = null;
            currentPair = getCurrent();
            if (currentPair == null) {
                break;
            }
            final Persistent23Tree<TreeNode<P, E>> queue = currentPair.getFirst().getClone();
            final PersistentHashSet<IdentifiedTreeNode<P, E>> values = currentPair.getSecond().getClone();
            final Persistent23Tree.MutableTree<TreeNode<P, E>> mutableQueue = queue.beginWrite();
            final PersistentHashSet.MutablePersistentHashSet<IdentifiedTreeNode<P, E>> mutableValues = values.beginWrite();
            final TreeNode<P, E> max = mutableQueue.getMaximum();
            if (max == null) {
                break;
            }
            mutableQueue.exclude(max);
            mutableValues.remove(new IdentifiedTreeNode<>(max));
            result = max.value;
            // commit trees and then try to commit pair of trees
            // no need to check endWrite() results since they commit cloned trees
            mutableQueue.endWrite();
            mutableValues.endWrite();
            // if the queue becomes empty the newPair reference can be null
            newPair = queue.isEmpty() ? null :
                    new Pair<>(queue, values);
            // commit pair if no other pair was already committed
        } while (!rootPair.compareAndSet(currentPair, newPair));

        return result;
    }

    @Override
    public void clear() {
        rootPair.set(null);
    }

    @Override
    public Guard lock() {
        return Guard.EMPTY;
    }

    @Override
    public void unlock() {
    }

    @Override
    public Iterator<E> iterator() {
        final Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> currentPair = getCurrent();
        if (currentPair == null) {
            final List<E> objects = Collections.emptyList();
            return objects.iterator();
        }
        final Iterator<TreeNode<P, E>> iterator = currentPair.getFirst().iterator();
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public E next() {
                return iterator.next().value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Nullable
    private Pair<Persistent23Tree<TreeNode<P, E>>, PersistentHashSet<IdentifiedTreeNode<P, E>>> getCurrent() {
        return rootPair.get();
    }

    @SuppressWarnings({"SubtractionInCompareTo", "ComparableImplementedButEqualsNotOverridden"})
    private static class TreeNode<P extends Comparable<? super P>, E> implements Comparable<TreeNode<P, E>> {

        private static final AtomicInteger orderCounter = new AtomicInteger(Integer.MAX_VALUE);

        private final P priority;
        private final int samePriorityOrder;
        private final E value;

        private TreeNode(final P priority, final int samePriorityOrder, final E value) {
            this.priority = priority;
            this.samePriorityOrder = samePriorityOrder;
            this.value = value;
        }

        private TreeNode(final P priority, final E value) {
            this(priority, orderCounter.getAndDecrement(), value);
        }

        @Override
        public int compareTo(final TreeNode<P, E> o) {
            int result = priority.compareTo(o.priority);
            if (result == 0) {
                result = samePriorityOrder - o.samePriorityOrder;
            }
            return result;
        }
    }

    private static class IdentifiedTreeNode<P extends Comparable<? super P>, E> {

        @NotNull
        private final TreeNode<P, E> node;

        private IdentifiedTreeNode(@NotNull final TreeNode<P, E> node) {
            this.node = node;
        }

        @Override
        public int hashCode() {
            return node.value.hashCode();
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(Object obj) {
            @SuppressWarnings("unchecked") final IdentifiedTreeNode<P, E> o = (IdentifiedTreeNode<P, E>) obj;
            return node.value.equals(o.node.value);
        }
    }
}
