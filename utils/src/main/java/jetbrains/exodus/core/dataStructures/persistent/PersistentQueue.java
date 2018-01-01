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

import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

@SuppressWarnings({"rawtypes", "unchecked"})
public class PersistentQueue<T> {

    public static final PersistentQueue EMPTY = new PersistentQueue();

    @NotNull
    private final PersistentStack<T> incoming;
    @NotNull
    // invariant: if outgoing is empty then incoming is also empty
    private final PersistentStack<T> outgoing;

    private PersistentQueue() {
        incoming = PersistentStack.EMPTY_STACK;
        outgoing = PersistentStack.EMPTY_STACK;
    }

    private PersistentQueue(PersistentStack<T> in, PersistentStack<T> out) {
        incoming = in;
        outgoing = out;
    }

    public int size() {
        return incoming.size() + outgoing.size();
    }

    public PersistentQueue<T> add(T element) {
        if (isEmpty()) {
            return new PersistentQueue<>(PersistentStack.EMPTY_STACK, PersistentStack.EMPTY_STACK.push(element));
        }
        return new PersistentQueue<>(incoming.push(element), outgoing);
    }

    public boolean isEmpty() {
        return outgoing.isEmpty();
    }

    public T peek() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return outgoing.peek();
    }

    public PersistentQueue<T> skip() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        final PersistentStack<T> out = outgoing.skip();
        if (out.isEmpty()) {
            return new PersistentQueue<>(PersistentStack.EMPTY_STACK, incoming.reverse());
        }
        return new PersistentQueue<>(incoming, out);
    }

    @Override
    public int hashCode() {
        return incoming.hashCode() + outgoing.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PersistentQueue)) {
            return false;
        }
        PersistentQueue<T> queue = (PersistentQueue<T>) obj;
        return incoming.equals(queue.incoming) && outgoing.equals(queue.outgoing);
    }
}
