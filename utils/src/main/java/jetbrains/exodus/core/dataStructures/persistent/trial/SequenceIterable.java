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

class SequenceIterable<T> implements Iterable<T> {

    private final Sequence<T> source;

    SequenceIterable(final Sequence<T> source) {
        this.source = source;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<>(source);
    }

    /*public static<T> Sequence<T> wrap(final Sequence<T> source)  {
        return source == null ? null : source.asSequence();
    }*/

    public static <T> T first(final Sequence<T> source) {
        return source == null ? null : source.first();
    }

    public static <T> Sequence<T> singleton(final T item) {
        return new Singleton<>(item);
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static final class Iterator<T> implements java.util.Iterator<T> {
        private boolean hasNextCalled;
        private Sequence<T> source;

        public Iterator(final Sequence<T> source) {
            this.source = source;
            hasNextCalled = false;
        }

        @Override
        public boolean hasNext() {
            if (hasNextCalled) {
                source = source.skip();
                hasNextCalled = false;
            }
            return source != null;
        }

        @Override
        public T next() {
            if (hasNext()) {
                final T result = source.first();
                hasNextCalled = true;
                return result;
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static final class Singleton<T> implements Sequence<T> {
        private final T item;

        public Singleton(T item) {
            this.item = item;
        }

        @Override
        public T first() {
            return item;
        }

        @Override
        public Sequence<T> skip() {
            return null;
        }
    }
}
