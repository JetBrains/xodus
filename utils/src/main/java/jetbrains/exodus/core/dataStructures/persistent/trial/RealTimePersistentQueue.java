/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.core.dataStructures.persistent.trial;

import java.util.Iterator;

public class RealTimePersistentQueue<T> implements Iterable<T> {

    public static final RealTimePersistentQueue EMPTY = new RealTimePersistentQueue(0, null, null);

    private final int size;
    private final Sequence<T> first;
    private final PersistentList<T> second;

    private RealTimePersistentQueue(int size, Sequence<T> first, PersistentList<T> second) {
        this.size = size;
        this.first = first;
        this.second = second;
    }

    @Override
    public Iterator<T> iterator() {
        return new SequenceIterable.Iterator<>(asSequence());
    }

    public Sequence<T> asSequence() {
        if (size == 0) {
            return null;
        }
        return new SequenceImpl<>(first, second == null ? null : second.asSequence());
    }

    public Object peek() {
        return SequenceIterable.first(first);
    }

    public RealTimePersistentQueue<T> pop() {
        if (first == null) {
            return this;
        }
        Sequence<T> f1 = first.skip();
        PersistentList<T> r1 = second;
        if (f1 == null) {
            f1 = second == null ? null : second.asSequence();
            r1 = null;
        }
        return new RealTimePersistentQueue<>(size - 1, f1, r1);
    }

    public RealTimePersistentQueue<T> add(T item) {
        if (first == null) {
            return new RealTimePersistentQueue<>(size + 1, SequenceIterable.singleton(item), null);
        } else {
            return new RealTimePersistentQueue<>(size + 1, first, (second != null ? second : PersistentList.EMPTY).add(item));
        }
    }

    public int getSize() {
        return size;
    }

    private static final class SequenceImpl<T> implements Sequence<T> {
        private final Sequence<T> first;
        private final Sequence<T> second;

        private SequenceImpl(Sequence<T> first, Sequence<T> second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public T first() {
            return first.first();
        }

        @Override
        public Sequence<T> skip() {
            final Sequence<T> first_ = first.skip();
            if (first_ == null) {
                if (second == null) {
                    return null;
                }
                return new SequenceImpl<>(second, null);
            } else {
                return new SequenceImpl<>(first_, second);
            }
        }
    }
}
